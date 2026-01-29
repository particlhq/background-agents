/**
 * Modal sandbox provider implementation.
 *
 * Wraps the existing ModalClient to implement the SandboxProvider interface,
 * enabling unit testing and future provider abstraction.
 */

import { generateInternalToken } from "@open-inspect/shared";
import type { ModalClient } from "../client";
import {
  SandboxProviderError,
  type SandboxProvider,
  type SandboxProviderCapabilities,
  type CreateSandboxConfig,
  type CreateSandboxResult,
  type RestoreConfig,
  type RestoreResult,
  type SnapshotConfig,
  type SnapshotResult,
} from "../provider";

/**
 * Modal sandbox provider.
 *
 * Implements the SandboxProvider interface using Modal's HTTP API.
 * All operations use HMAC-authenticated requests via the shared secret.
 *
 * @example
 * ```typescript
 * const client = createModalClient(secret, workspace);
 * const provider = new ModalSandboxProvider(client, secret);
 *
 * try {
 *   const result = await provider.createSandbox(config);
 * } catch (e) {
 *   if (e instanceof SandboxProviderError && e.errorType === "permanent") {
 *     // Increment circuit breaker
 *   }
 * }
 * ```
 */
export class ModalSandboxProvider implements SandboxProvider {
  readonly name = "modal";

  readonly capabilities: SandboxProviderCapabilities = {
    supportsSnapshots: true,
    supportsRestore: true,
    supportsWarm: true,
  };

  constructor(
    private readonly client: ModalClient,
    private readonly secret: string
  ) {}

  /**
   * Create a new sandbox via Modal API.
   */
  async createSandbox(config: CreateSandboxConfig): Promise<CreateSandboxResult> {
    try {
      const result = await this.client.createSandbox({
        sessionId: config.sessionId,
        sandboxId: config.sandboxId,
        repoOwner: config.repoOwner,
        repoName: config.repoName,
        controlPlaneUrl: config.controlPlaneUrl,
        sandboxAuthToken: config.sandboxAuthToken,
        opencodeSessionId: config.opencodeSessionId,
        gitUserName: config.gitUserName,
        gitUserEmail: config.gitUserEmail,
        provider: config.provider,
        model: config.model,
      });

      return {
        sandboxId: result.sandboxId,
        providerObjectId: result.modalObjectId,
        status: result.status,
        createdAt: result.createdAt,
      };
    } catch (error) {
      throw this.classifyError("Failed to create sandbox", error);
    }
  }

  /**
   * Restore a sandbox from a filesystem snapshot.
   */
  async restoreFromSnapshot(config: RestoreConfig): Promise<RestoreResult> {
    try {
      const restoreUrl = this.client.getRestoreSandboxUrl();
      const authToken = await generateInternalToken(this.secret);

      const response = await fetch(restoreUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken}`,
        },
        body: JSON.stringify({
          snapshot_image_id: config.snapshotImageId,
          session_config: {
            session_id: config.sessionId,
            repo_owner: config.repoOwner,
            repo_name: config.repoName,
            provider: config.provider,
            model: config.model,
          },
          sandbox_id: config.sandboxId,
          control_plane_url: config.controlPlaneUrl,
          sandbox_auth_token: config.sandboxAuthToken,
        }),
      });

      const result = (await response.json()) as {
        success: boolean;
        data?: { sandbox_id: string };
        error?: string;
      };

      if (result.success) {
        return {
          success: true,
          sandboxId: result.data?.sandbox_id,
        };
      }

      return {
        success: false,
        error: result.error || "Unknown restore error",
      };
    } catch (error) {
      throw this.classifyError("Failed to restore sandbox from snapshot", error);
    }
  }

  /**
   * Take a filesystem snapshot of the sandbox.
   */
  async takeSnapshot(config: SnapshotConfig): Promise<SnapshotResult> {
    try {
      const snapshotUrl = this.client.getSnapshotSandboxUrl();
      const authToken = await generateInternalToken(this.secret);

      const response = await fetch(snapshotUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken}`,
        },
        body: JSON.stringify({
          sandbox_id: config.providerObjectId, // Modal's internal object ID
          session_id: config.sessionId,
          reason: config.reason,
        }),
      });

      const result = (await response.json()) as {
        success: boolean;
        data?: { image_id: string };
        error?: string;
      };

      if (result.success && result.data?.image_id) {
        return {
          success: true,
          imageId: result.data.image_id,
        };
      }

      return {
        success: false,
        error: result.error || "Unknown snapshot error",
      };
    } catch (error) {
      throw this.classifyError("Failed to take snapshot", error);
    }
  }

  /**
   * Classify an error as transient or permanent for circuit breaker handling.
   */
  private classifyError(message: string, error: unknown): SandboxProviderError {
    // Check for fetch/network errors
    if (error instanceof Error) {
      const errorMessage = error.message.toLowerCase();

      // Transient network errors
      if (
        errorMessage.includes("fetch failed") ||
        errorMessage.includes("etimedout") ||
        errorMessage.includes("econnreset") ||
        errorMessage.includes("econnrefused") ||
        errorMessage.includes("network") ||
        errorMessage.includes("timeout") ||
        errorMessage.includes("502") ||
        errorMessage.includes("503") ||
        errorMessage.includes("504") ||
        errorMessage.includes("bad gateway") ||
        errorMessage.includes("service unavailable") ||
        errorMessage.includes("gateway timeout")
      ) {
        return new SandboxProviderError(`${message}: ${error.message}`, "transient", error);
      }
    }

    // Default to permanent for unknown errors (config issues, auth failures, etc.)
    return new SandboxProviderError(
      `${message}: ${error instanceof Error ? error.message : String(error)}`,
      "permanent",
      error instanceof Error ? error : undefined
    );
  }
}

/**
 * Create a Modal sandbox provider.
 *
 * @param client - ModalClient instance for API calls
 * @param secret - MODAL_API_SECRET for authentication
 * @returns ModalSandboxProvider instance
 */
export function createModalProvider(client: ModalClient, secret: string): ModalSandboxProvider {
  return new ModalSandboxProvider(client, secret);
}
