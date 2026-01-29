/**
 * SandboxLifecycleManager - orchestrates sandbox lifecycle operations.
 *
 * This class coordinates spawn, restore, snapshot, and timeout logic by:
 * 1. Using pure decision functions to make decisions (no side effects)
 * 2. Executing side effects through injected dependencies (storage, broadcast, etc.)
 * 3. Delegating provider operations to the SandboxProvider abstraction
 *
 * The manager owns the in-memory `isSpawningSandbox` flag to prevent concurrent
 * spawn attempts within the same request.
 */

import type { SandboxStatus } from "../../types";
import type { SandboxRow, SessionRow } from "../../session/types";
import { SandboxProviderError, type SandboxProvider, type CreateSandboxConfig } from "../provider";
import {
  evaluateCircuitBreaker,
  evaluateSpawnDecision,
  evaluateInactivityTimeout,
  evaluateHeartbeatHealth,
  evaluateWarmDecision,
  DEFAULT_CIRCUIT_BREAKER_CONFIG,
  DEFAULT_SPAWN_CONFIG,
  DEFAULT_INACTIVITY_CONFIG,
  DEFAULT_HEARTBEAT_CONFIG,
  type CircuitBreakerConfig,
  type SpawnConfig,
  type InactivityConfig,
  type HeartbeatConfig,
} from "./decisions";

// ==================== Dependency Interfaces ====================

/**
 * Sandbox state with circuit breaker info (subset of full SandboxRow).
 */
export interface SandboxCircuitBreakerInfo {
  status: string;
  created_at: number;
  snapshot_image_id: string | null;
  spawn_failure_count: number | null;
  last_spawn_failure: number | null;
}

/**
 * Storage adapter for sandbox data operations.
 */
export interface SandboxStorage {
  /** Get current sandbox state */
  getSandbox(): SandboxRow | null;
  /** Get sandbox with circuit breaker state (subset of fields) */
  getSandboxWithCircuitBreaker(): SandboxCircuitBreakerInfo | null;
  /** Get current session */
  getSession(): SessionRow | null;
  /** Update sandbox status */
  updateSandboxStatus(status: SandboxStatus): void;
  /** Update sandbox for spawn (status, auth token, sandbox ID, created_at) */
  updateSandboxForSpawn(data: {
    status: SandboxStatus;
    createdAt: number;
    authToken: string;
    modalSandboxId: string;
  }): void;
  /** Update sandbox Modal object ID (for snapshot API) */
  updateSandboxModalObjectId(modalObjectId: string): void;
  /** Update sandbox snapshot image ID */
  updateSandboxSnapshotImageId(sandboxId: string, imageId: string): void;
  /** Update last activity timestamp */
  updateSandboxLastActivity(timestamp: number): void;
  /** Increment circuit breaker failure count */
  incrementCircuitBreakerFailure(timestamp: number): void;
  /** Reset circuit breaker failure count */
  resetCircuitBreaker(): void;
}

/**
 * Broadcaster for sending messages to connected clients.
 */
export interface SandboxBroadcaster {
  /** Broadcast a message to all connected clients */
  broadcast(message: object): void;
}

/**
 * WebSocket manager for sandbox communication.
 */
export interface WebSocketManager {
  /** Get the sandbox WebSocket (with hibernation recovery) */
  getSandboxWebSocket(): WebSocket | null;
  /** Close the sandbox WebSocket */
  closeSandboxWebSocket(code: number, reason: string): void;
  /** Send a message to the sandbox */
  sendToSandbox(message: object): boolean;
  /** Get count of connected client WebSockets (excludes sandbox) */
  getConnectedClientCount(): number;
}

/**
 * Alarm scheduler for timeouts.
 */
export interface AlarmScheduler {
  /** Schedule an alarm at the given timestamp */
  scheduleAlarm(timestamp: number): Promise<void>;
}

/**
 * ID generator for sandbox and token IDs.
 */
export interface IdGenerator {
  /** Generate a unique ID */
  generateId(): string;
}

// ==================== Configuration ====================

/**
 * Complete lifecycle configuration.
 */
export interface SandboxLifecycleConfig {
  circuitBreaker: CircuitBreakerConfig;
  spawn: SpawnConfig;
  inactivity: InactivityConfig;
  heartbeat: HeartbeatConfig;
  controlPlaneUrl: string;
  provider: string;
  model: string;
}

/**
 * Default lifecycle configuration.
 */
export const DEFAULT_LIFECYCLE_CONFIG: Omit<
  SandboxLifecycleConfig,
  "controlPlaneUrl" | "provider" | "model"
> = {
  circuitBreaker: DEFAULT_CIRCUIT_BREAKER_CONFIG,
  spawn: DEFAULT_SPAWN_CONFIG,
  inactivity: DEFAULT_INACTIVITY_CONFIG,
  heartbeat: DEFAULT_HEARTBEAT_CONFIG,
};

// ==================== Manager ====================

/**
 * Manages sandbox lifecycle operations.
 *
 * Uses dependency injection for all external interactions, enabling unit testing
 * with mocked dependencies.
 */
export class SandboxLifecycleManager {
  /**
   * In-memory flag to prevent concurrent spawn attempts within the same request.
   * This is NOT persisted - it protects against multiple spawns in one DO method call.
   * The persisted sandbox status ("spawning", "connecting") handles cross-request protection.
   */
  private isSpawningSandbox = false;

  constructor(
    private readonly provider: SandboxProvider,
    private readonly storage: SandboxStorage,
    private readonly broadcaster: SandboxBroadcaster,
    private readonly wsManager: WebSocketManager,
    private readonly alarmScheduler: AlarmScheduler,
    private readonly idGenerator: IdGenerator,
    private readonly config: SandboxLifecycleConfig
  ) {}

  /**
   * Spawn a sandbox (fresh or from snapshot).
   *
   * Uses decision functions to determine the appropriate action:
   * - Check circuit breaker
   * - Restore from snapshot if available and sandbox is stopped/stale/failed
   * - Fresh spawn if all conditions pass
   */
  async spawnSandbox(): Promise<void> {
    const sandboxState = this.storage.getSandboxWithCircuitBreaker();
    const now = Date.now();

    // Extract circuit breaker state
    const circuitBreakerState = {
      failureCount: sandboxState?.spawn_failure_count || 0,
      lastFailureTime: sandboxState?.last_spawn_failure || 0,
    };

    // Check circuit breaker
    const cbDecision = evaluateCircuitBreaker(circuitBreakerState, this.config.circuitBreaker, now);

    if (cbDecision.shouldReset) {
      console.log("[Manager] Circuit breaker window passed, resetting failure count");
      this.storage.resetCircuitBreaker();
    }

    if (!cbDecision.shouldProceed) {
      console.log(
        `[Manager] Circuit breaker open: ${circuitBreakerState.failureCount} failures, wait ${Math.ceil((cbDecision.waitTimeMs || 0) / 1000)}s`
      );
      this.broadcaster.broadcast({
        type: "sandbox_error",
        error: `Sandbox spawning temporarily disabled after ${circuitBreakerState.failureCount} failures. Try again in ${Math.ceil((cbDecision.waitTimeMs || 0) / 1000)} seconds.`,
      });
      return;
    }

    // Evaluate spawn decision
    const spawnState = {
      status: (sandboxState?.status || "pending") as SandboxStatus,
      createdAt: sandboxState?.created_at || 0,
      snapshotImageId: sandboxState?.snapshot_image_id || null,
      hasActiveWebSocket: this.wsManager.getSandboxWebSocket() !== null,
    };

    const spawnDecision = evaluateSpawnDecision(
      spawnState,
      this.config.spawn,
      now,
      this.isSpawningSandbox
    );

    switch (spawnDecision.action) {
      case "skip":
        console.log(`[Manager] spawnSandbox: ${spawnDecision.reason}`);
        return;

      case "wait":
        console.log(`[Manager] spawnSandbox: ${spawnDecision.reason}`);
        return;

      case "restore":
        console.log(`[Manager] Restoring from snapshot: ${spawnDecision.snapshotImageId}`);
        await this.restoreFromSnapshot(spawnDecision.snapshotImageId);
        return;

      case "spawn":
        await this.doSpawn();
        return;
    }
  }

  /**
   * Execute a fresh sandbox spawn.
   */
  private async doSpawn(): Promise<void> {
    this.isSpawningSandbox = true;

    try {
      const session = this.storage.getSession();
      if (!session) {
        console.error("[Manager] Cannot spawn sandbox: no session");
        return;
      }

      const now = Date.now();
      const sessionId = session.session_name || session.id;
      const sandboxAuthToken = this.idGenerator.generateId();
      const expectedSandboxId = `sandbox-${session.repo_owner}-${session.repo_name}-${now}`;

      // Store expected sandbox ID and auth token BEFORE calling provider
      this.storage.updateSandboxForSpawn({
        status: "spawning",
        createdAt: now,
        authToken: sandboxAuthToken,
        modalSandboxId: expectedSandboxId,
      });
      this.broadcaster.broadcast({ type: "sandbox_status", status: "spawning" });

      console.log(`[Manager] Creating sandbox: ${expectedSandboxId}`);

      // Create sandbox via provider
      const createConfig: CreateSandboxConfig = {
        sessionId,
        sandboxId: expectedSandboxId,
        repoOwner: session.repo_owner,
        repoName: session.repo_name,
        controlPlaneUrl: this.config.controlPlaneUrl,
        sandboxAuthToken,
        provider: this.config.provider,
        model: session.model || this.config.model,
      };

      const result = await this.provider.createSandbox(createConfig);

      console.log("[Manager] Sandbox created:", result.sandboxId);

      // Store provider's internal object ID for snapshot API
      if (result.providerObjectId) {
        this.storage.updateSandboxModalObjectId(result.providerObjectId);
        console.log(`[Manager] Stored provider object ID: ${result.providerObjectId}`);
      }

      this.storage.updateSandboxStatus("connecting");
      this.broadcaster.broadcast({ type: "sandbox_status", status: "connecting" });

      // Reset circuit breaker on successful spawn initiation
      this.storage.resetCircuitBreaker();
    } catch (error) {
      console.error("[Manager] Failed to spawn sandbox:", error);

      // Only increment circuit breaker for permanent errors
      if (error instanceof SandboxProviderError) {
        if (error.errorType === "permanent") {
          this.storage.incrementCircuitBreakerFailure(Date.now());
          console.log("[Manager] Incremented spawn failure count (permanent error)");
        } else {
          console.log("[Manager] Transient error, not incrementing circuit breaker");
        }
      } else {
        // Unknown error type - treat as permanent
        this.storage.incrementCircuitBreakerFailure(Date.now());
        console.log("[Manager] Incremented spawn failure count (unknown error)");
      }

      this.storage.updateSandboxStatus("failed");
      this.broadcaster.broadcast({
        type: "sandbox_error",
        error: error instanceof Error ? error.message : "Failed to spawn sandbox",
      });
    } finally {
      this.isSpawningSandbox = false;
    }
  }

  /**
   * Restore a sandbox from a filesystem snapshot.
   */
  private async restoreFromSnapshot(snapshotImageId: string): Promise<void> {
    if (!this.provider.restoreFromSnapshot) {
      console.log("[Manager] Provider does not support restore");
      // Fall back to fresh spawn
      await this.doSpawn();
      return;
    }

    this.isSpawningSandbox = true;

    try {
      const session = this.storage.getSession();
      if (!session) {
        console.error("[Manager] Cannot restore: no session");
        return;
      }

      this.storage.updateSandboxStatus("spawning");
      this.broadcaster.broadcast({ type: "sandbox_status", status: "spawning" });

      const now = Date.now();
      const sandboxAuthToken = this.idGenerator.generateId();
      const expectedSandboxId = `sandbox-${session.repo_owner}-${session.repo_name}-${now}`;

      // Store expected sandbox ID and auth token
      this.storage.updateSandboxForSpawn({
        status: "spawning",
        createdAt: now,
        authToken: sandboxAuthToken,
        modalSandboxId: expectedSandboxId,
      });

      console.log(`[Manager] Restoring sandbox from snapshot: ${snapshotImageId}`);

      const result = await this.provider.restoreFromSnapshot({
        snapshotImageId,
        sessionId: session.session_name || session.id,
        sandboxId: expectedSandboxId,
        sandboxAuthToken,
        controlPlaneUrl: this.config.controlPlaneUrl,
        repoOwner: session.repo_owner,
        repoName: session.repo_name,
        provider: this.config.provider,
        model: session.model || this.config.model,
      });

      if (result.success) {
        console.log(`[Manager] Sandbox restored: ${result.sandboxId}`);
        this.storage.updateSandboxStatus("connecting");
        this.broadcaster.broadcast({ type: "sandbox_status", status: "connecting" });
        this.broadcaster.broadcast({
          type: "sandbox_restored",
          message: "Session restored from snapshot",
        });
      } else {
        console.error("[Manager] Restore failed:", result.error);
        this.storage.updateSandboxStatus("failed");
        this.broadcaster.broadcast({
          type: "sandbox_error",
          error: result.error || "Failed to restore from snapshot",
        });
      }
    } catch (error) {
      console.error("[Manager] Restore request failed:", error);
      this.storage.updateSandboxStatus("failed");
      this.broadcaster.broadcast({
        type: "sandbox_error",
        error: error instanceof Error ? error.message : "Failed to restore sandbox",
      });
    } finally {
      this.isSpawningSandbox = false;
    }
  }

  /**
   * Trigger a filesystem snapshot of the sandbox.
   */
  async triggerSnapshot(reason: string): Promise<void> {
    if (!this.provider.takeSnapshot) {
      console.log("[Manager] Provider does not support snapshots");
      return;
    }

    const sandbox = this.storage.getSandbox();
    const session = this.storage.getSession();

    if (!sandbox?.modal_object_id || !session) {
      console.log("[Manager] Cannot snapshot: no modal_object_id or session");
      return;
    }

    // Don't snapshot if already snapshotting
    if (sandbox.status === "snapshotting") {
      console.log("[Manager] Already snapshotting, skipping");
      return;
    }

    // Track previous status for non-terminal states
    const isTerminalState =
      sandbox.status === "stopped" || sandbox.status === "stale" || sandbox.status === "failed";
    const previousStatus = sandbox.status;

    if (!isTerminalState) {
      this.storage.updateSandboxStatus("snapshotting");
      this.broadcaster.broadcast({ type: "sandbox_status", status: "snapshotting" });
    }

    try {
      console.log(`[Manager] Triggering snapshot, reason: ${reason}`);

      const result = await this.provider.takeSnapshot({
        providerObjectId: sandbox.modal_object_id,
        sessionId: session.session_name || session.id,
        reason,
      });

      if (result.success && result.imageId) {
        this.storage.updateSandboxSnapshotImageId(sandbox.id, result.imageId);
        console.log(`[Manager] Snapshot saved: ${result.imageId}`);
        this.broadcaster.broadcast({
          type: "snapshot_saved",
          imageId: result.imageId,
          reason,
        });
      } else {
        console.error("[Manager] Snapshot failed:", result.error);
      }
    } catch (error) {
      console.error("[Manager] Snapshot request failed:", error);
    }

    // Restore previous status if we weren't in a terminal state
    if (!isTerminalState && reason !== "heartbeat_timeout") {
      this.storage.updateSandboxStatus(previousStatus as SandboxStatus);
      this.broadcaster.broadcast({ type: "sandbox_status", status: previousStatus });
    }
  }

  /**
   * Handle alarm for inactivity and heartbeat monitoring.
   */
  async handleAlarm(): Promise<void> {
    console.log("[Manager] ===== ALARM FIRED =====");

    const sandbox = this.storage.getSandbox();
    if (!sandbox) {
      console.log("[Manager] Alarm: no sandbox found");
      return;
    }

    const now = Date.now();

    console.log(
      `[Manager] Alarm: status=${sandbox.status}, last_activity=${sandbox.last_activity}, last_heartbeat=${sandbox.last_heartbeat}`
    );

    // Skip if sandbox is already in terminal state
    if (sandbox.status === "stopped" || sandbox.status === "failed" || sandbox.status === "stale") {
      console.log(`[Manager] Alarm: sandbox status is ${sandbox.status}, skipping`);
      return;
    }

    // Check heartbeat health first
    const heartbeatHealth = evaluateHeartbeatHealth(
      sandbox.last_heartbeat,
      this.config.heartbeat,
      now
    );

    if (heartbeatHealth.isStale) {
      console.log(
        `[Manager] Heartbeat timeout: ${(heartbeatHealth.ageMs || 0) / 1000}s since last heartbeat`
      );
      // Fire-and-forget snapshot so status broadcast isn't delayed
      this.triggerSnapshot("heartbeat_timeout").catch((e) =>
        console.error("[Manager] Heartbeat snapshot failed:", e)
      );
      this.storage.updateSandboxStatus("stale");
      this.broadcaster.broadcast({ type: "sandbox_status", status: "stale" });
      return;
    }

    // Evaluate inactivity timeout
    const connectedClients = this.getConnectedClientCount();
    const inactivityState = {
      lastActivity: sandbox.last_activity,
      status: sandbox.status as SandboxStatus,
      connectedClientCount: connectedClients,
    };

    const inactivityDecision = evaluateInactivityTimeout(
      inactivityState,
      this.config.inactivity,
      now
    );

    switch (inactivityDecision.action) {
      case "timeout":
        console.log("[Manager] Inactivity timeout, triggering stop");
        // Set status to stopped FIRST to block reconnection attempts
        this.storage.updateSandboxStatus("stopped");
        this.broadcaster.broadcast({ type: "sandbox_status", status: "stopped" });
        console.log("[Manager] Status set to stopped, blocking reconnections");

        // Take snapshot
        await this.triggerSnapshot("inactivity_timeout");

        // Send shutdown command and close WebSocket
        this.wsManager.sendToSandbox({ type: "shutdown" });
        this.wsManager.closeSandboxWebSocket(1000, "Inactivity timeout");

        this.broadcaster.broadcast({
          type: "sandbox_warning",
          message: "Sandbox stopped due to inactivity, snapshot saved",
        });
        return;

      case "extend":
        console.log(
          `[Manager] Inactivity timeout but ${connectedClients} clients connected, extending`
        );
        if (inactivityDecision.shouldWarn) {
          this.broadcaster.broadcast({
            type: "sandbox_warning",
            message:
              "Sandbox will stop in 5 minutes due to inactivity. Send a message to keep it alive.",
          });
        }
        await this.alarmScheduler.scheduleAlarm(now + inactivityDecision.extensionMs);
        return;

      case "schedule":
        console.log(`[Manager] Scheduling next alarm in ${inactivityDecision.nextCheckMs / 1000}s`);
        await this.alarmScheduler.scheduleAlarm(now + inactivityDecision.nextCheckMs);
        return;
    }
  }

  /**
   * Warm sandbox proactively (e.g., when user starts typing).
   */
  async warmSandbox(): Promise<void> {
    const sandbox = this.storage.getSandbox();

    const warmState = {
      hasActiveWebSocket: this.wsManager.getSandboxWebSocket() !== null,
      status: sandbox?.status as SandboxStatus | null,
      isSpawningInMemory: this.isSpawningSandbox,
    };

    const warmDecision = evaluateWarmDecision(warmState);

    if (warmDecision.action === "skip") {
      console.log(`[Manager] warmSandbox: ${warmDecision.reason}`);
      return;
    }

    console.log("[Manager] Warming sandbox proactively");
    this.broadcaster.broadcast({ type: "sandbox_warming" });
    await this.spawnSandbox();
  }

  /**
   * Update last activity timestamp.
   */
  updateLastActivity(timestamp: number): void {
    this.storage.updateSandboxLastActivity(timestamp);
  }

  /**
   * Schedule an inactivity check alarm.
   */
  async scheduleInactivityCheck(): Promise<void> {
    const alarmTime = Date.now() + this.config.inactivity.timeoutMs;
    console.log(
      `[Manager] Scheduling inactivity check in ${this.config.inactivity.timeoutMs / 1000}s`
    );
    await this.alarmScheduler.scheduleAlarm(alarmTime);
  }

  /**
   * Get the count of connected client WebSockets.
   */
  private getConnectedClientCount(): number {
    return this.wsManager.getConnectedClientCount();
  }
}
