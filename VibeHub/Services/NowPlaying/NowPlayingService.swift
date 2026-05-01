#if !APP_STORE

import AppKit
import Combine
import Foundation
import os.log

private let logger = Logger(subsystem: "com.vibehub", category: "NowPlaying")

/// Event-driven Spotify now-playing tracker.
///
/// Listens to Spotify's `PlaybackStateChanged` broadcast and `NSWorkspace`
/// launch/terminate events instead of polling. AppleScript is only invoked
/// in response to a real event, and never when Spotify isn't running.
actor NowPlayingService {
    static let shared = NowPlayingService()

    private let executor = ProcessExecutor.shared

    let stateSubject = CurrentValueSubject<NowPlayingState, Never>(.empty)

    /// Album art cache keyed by artwork URL
    private var artworkCache: [String: NSImage] = [:]
    private var lastArtworkURL: String?

    private var spotifyNotificationTask: Task<Void, Never>?
    private var workspaceLaunchTask: Task<Void, Never>?
    private var workspaceTerminateTask: Task<Void, Never>?

    private static let spotifyBundleID = "com.spotify.client"

    private init() {}

    // MARK: - Lifecycle

    func start() {
        logger.info("NowPlayingService started (Spotify)")
        cancelTasks()

        // Spotify broadcasts this on every track/play-state change.
        let spotifyEvents = DistributedNotificationCenter.default()
            .notifications(named: Notification.Name("com.spotify.client.PlaybackStateChanged"))
        spotifyNotificationTask = Task { [weak self] in
            for await _ in spotifyEvents {
                guard let self else { return }
                await self.refresh()
            }
        }

        // Refresh when Spotify launches; clear when it quits — cheaper than
        // probing `runningApplications` on every event.
        let launches = NSWorkspace.shared.notificationCenter
            .notifications(named: NSWorkspace.didLaunchApplicationNotification)
        workspaceLaunchTask = Task { [weak self] in
            for await note in launches {
                guard
                    let self,
                    let app = note.userInfo?[NSWorkspace.applicationUserInfoKey] as? NSRunningApplication,
                    app.bundleIdentifier == Self.spotifyBundleID
                else { continue }
                await self.refresh()
            }
        }

        let terminations = NSWorkspace.shared.notificationCenter
            .notifications(named: NSWorkspace.didTerminateApplicationNotification)
        workspaceTerminateTask = Task { [weak self] in
            for await note in terminations {
                guard
                    let self,
                    let app = note.userInfo?[NSWorkspace.applicationUserInfoKey] as? NSRunningApplication,
                    app.bundleIdentifier == Self.spotifyBundleID
                else { continue }
                await self.clear()
            }
        }

        // Initial state — only spawns osascript if Spotify is already running.
        Task { [weak self] in await self?.refresh() }
    }

    func stop() {
        cancelTasks()
    }

    private func cancelTasks() {
        spotifyNotificationTask?.cancel(); spotifyNotificationTask = nil
        workspaceLaunchTask?.cancel(); workspaceLaunchTask = nil
        workspaceTerminateTask?.cancel(); workspaceTerminateTask = nil
    }

    private nonisolated func isSpotifyRunning() -> Bool {
        NSWorkspace.shared.runningApplications.contains { $0.bundleIdentifier == Self.spotifyBundleID }
    }

    private func refresh() async {
        guard isSpotifyRunning() else {
            clear()
            return
        }
        await poll()
    }

    private func clear() {
        if stateSubject.value.hasMedia {
            stateSubject.send(.empty)
        }
        lastArtworkURL = nil
    }

    // MARK: - Polling

    /// Single AppleScript that returns all fields separated by |||
    private static let pollScript = """
    if application "Spotify" is running then
        tell application "Spotify"
            if player state is not stopped then
                set t to name of current track
                set a to artist of current track
                set al to album of current track
                set art to artwork url of current track
                set s to player state as string
                set d to duration of current track
                set p to player position
                return t & "|||" & a & "|||" & al & "|||" & art & "|||" & s & "|||" & d & "|||" & p
            end if
        end tell
    end if
    """

    private func poll() async {
        let result = await executor.runWithResult(
            "/usr/bin/osascript",
            arguments: ["-e", Self.pollScript],
            timeoutSeconds: 3
        )

        switch result {
        case .success(let process):
            let output = process.output.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !output.isEmpty else {
                stateSubject.send(.empty)
                return
            }

            let parts = output.components(separatedBy: "|||")
            guard parts.count >= 7 else {
                stateSubject.send(.empty)
                return
            }

            let title = parts[0]
            let artist = parts[1]
            let album = parts[2]
            let artworkURL = parts[3]
            let playerState = parts[4]  // "playing", "paused"
            let durationMs = Double(parts[5]) ?? 0
            let position = Double(parts[6]) ?? 0

            // Fetch artwork when URL changes
            if artworkURL != lastArtworkURL, !artworkURL.isEmpty {
                lastArtworkURL = artworkURL
                if artworkCache[artworkURL] == nil {
                    let artwork = await fetchArtwork(url: artworkURL)
                    if let artwork { artworkCache[artworkURL] = artwork }
                }
            }

            let state = NowPlayingState(
                title: title,
                artist: artist,
                album: album,
                artwork: artworkCache[artworkURL],
                isPlaying: playerState == "playing",
                duration: durationMs / 1000.0,
                elapsed: position
            )
            stateSubject.send(state)

        case .failure:
            stateSubject.send(.empty)
        }
    }

    // MARK: - Artwork

    private func fetchArtwork(url: String) async -> NSImage? {
        guard let imageURL = URL(string: url) else { return nil }
        do {
            let (data, _) = try await URLSession.shared.data(from: imageURL)
            return NSImage(data: data)
        } catch {
            return nil
        }
    }

    // MARK: - Controls

    func togglePlayPause() async {
        guard isSpotifyRunning() else { return }
        _ = await executor.runWithResult(
            "/usr/bin/osascript",
            arguments: ["-e", "tell application \"Spotify\" to playpause"],
            timeoutSeconds: 2
        )
    }

    func next() async {
        guard isSpotifyRunning() else { return }
        _ = await executor.runWithResult(
            "/usr/bin/osascript",
            arguments: ["-e", "tell application \"Spotify\" to next track"],
            timeoutSeconds: 2
        )
        await refresh()
    }

    func previous() async {
        guard isSpotifyRunning() else { return }
        _ = await executor.runWithResult(
            "/usr/bin/osascript",
            arguments: ["-e", "tell application \"Spotify\" to previous track"],
            timeoutSeconds: 2
        )
        await refresh()
    }
}

#endif
