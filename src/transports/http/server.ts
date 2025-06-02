import { randomUUID } from 'node:crypto';
import { IncomingMessage, ServerResponse, createServer, Server as HttpServer } from 'node:http';
import { AbstractTransport } from '../base.js';
import { JSONRPCMessage, isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { HttpStreamTransportConfig } from './types.js';
import { logger } from '../../core/Logger.js';

export class HttpStreamTransport extends AbstractTransport {
  readonly type = 'http-stream';
  private _sdkTransport: StreamableHTTPServerTransport;
  private _isRunning = false;
  private _port: number;
  private _host: string;
  private _server?: HttpServer;
  private _endpoint: string;
  private _enableJsonResponse: boolean = false;
  private _sessionInitialized: boolean = false;

  private _pingInterval?: NodeJS.Timeout;
  private _pingTimeouts: Map<string | number, NodeJS.Timeout> = new Map();
  private _pingFrequency: number;
  private _pingTimeout: number;

  constructor(config: HttpStreamTransportConfig = {}) {
    super();

    this._port = config.port || 8080;
    this._host = config.host || '127.0.0.1';
    this._endpoint = config.endpoint || '/mcp';
    this._enableJsonResponse = config.responseMode === 'batch';

    this._pingFrequency = config.ping?.frequency ?? 30000; // Default 30 seconds
    this._pingTimeout = config.ping?.timeout ?? 10000; // Default 10 seconds

    this._sdkTransport = this.createSdkTransport();

    logger.debug(
      `HttpStreamTransport configured with: ${JSON.stringify({
        port: this._port,
        endpoint: this._endpoint,
        responseMode: config.responseMode,
        batchTimeout: config.batchTimeout,
        maxMessageSize: config.maxMessageSize,
        auth: config.auth ? true : false,
        cors: config.cors ? true : false,
        ping: {
          frequency: this._pingFrequency,
          timeout: this._pingTimeout,
        },
      })}`
    );
  }

  private createSdkTransport(): StreamableHTTPServerTransport {
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      onsessioninitialized: (sessionId: string) => {
        logger.info(`Session initialized: ${sessionId}`);
        this._sessionInitialized = true;
      },
      enableJsonResponse: this._enableJsonResponse,
    });

    transport.onmessage = (message: JSONRPCMessage) => {
      if (this.handlePingMessage(message)) {
        return;
      }

      this._onmessage?.(message);
    };

    return transport;
  }

  async start(): Promise<void> {
    if (this._isRunning) {
      throw new Error('HttpStreamTransport already started');
    }

    return new Promise((resolve, reject) => {
      this._server = createServer(async (req, res) => {
        try {
          const url = new URL(req.url!, `http://${req.headers.host}`);

          if (url.pathname === this._endpoint) {
            // Special handling for POST requests to detect initialization requests
            if (req.method === 'POST') {
              const contentType = req.headers['content-type'];
              if (contentType?.includes('application/json')) {
                // Need to intercept body data to check for initialization
                let bodyData = '';
                req.on('data', (chunk) => {
                  bodyData += chunk.toString();
                });

                req.on('end', async () => {
                  try {
                    const jsonData = JSON.parse(bodyData);
                    const messages = Array.isArray(jsonData) ? jsonData : [jsonData];

                    // Check if this is an initialization request AND we already have a session
                    // Only recreate for subsequent initializations, not the first one
                    if (messages.some(isInitializeRequest) && this._sessionInitialized) {
                      logger.info(
                        'Received initialization request for existing session, recreating transport'
                      );

                      // Reset session state first
                      this._sessionInitialized = false;

                      // Close the old transport
                      try {
                        await this._sdkTransport.close();
                      } catch (err) {
                        logger.warn(`Error closing previous transport: ${err}`);
                      }

                      // Create a fresh transport for this new connection
                      this._sdkTransport = this.createSdkTransport();
                      await this._sdkTransport.start();
                    }

                    // Forward the original request to the SDK transport
                    await this._sdkTransport.handleRequest(req, res, jsonData);
                  } catch (error) {
                    logger.error(`Error handling JSON data: ${error}`);
                    if (!res.headersSent) {
                      res.writeHead(400).end(
                        JSON.stringify({
                          jsonrpc: '2.0',
                          error: {
                            code: -32700,
                            message: 'Parse error',
                            data: String(error),
                          },
                          id: null,
                        })
                      );
                    }
                  }
                });
              } else {
                await this._sdkTransport.handleRequest(req, res);
              }
            } else if (req.method === 'DELETE') {
              // For DELETE requests, reset the session state
              this._sessionInitialized = false;
              await this._sdkTransport.handleRequest(req, res);
            } else {
              // For GET requests, just forward to the SDK transport
              await this._sdkTransport.handleRequest(req, res);
            }
          } else {
            res.writeHead(404).end('Not Found');
          }
        } catch (error) {
          logger.error(`Error handling request: ${error}`);
          if (!res.headersSent) {
            res.writeHead(500).end('Internal Server Error');
          }
        }
      });

      this._server.on('error', (error) => {
        logger.error(`HTTP server error: ${error}`);
        this._onerror?.(error);
        if (!this._isRunning) {
          reject(error);
        }
      });

      this._server.on('close', () => {
        logger.info('HTTP server closed');
        this._isRunning = false;
        this._onclose?.();
      });

      this._server.listen(this._port, this._host, () => {
        logger.info(`HTTP server listening on port ${this._port}, endpoint ${this._endpoint}`);

        this._sdkTransport
          .start()
          .then(() => {
            this._isRunning = true;
            logger.info(`HttpStreamTransport started successfully on port ${this._port}`);

            this.startPingInterval();

            resolve();
          })
          .catch((error) => {
            logger.error(`Failed to start SDK transport: ${error}`);
            this._server?.close();
            reject(error);
          });
      });
    });
  }

  private startPingInterval(): void {
    if (this._pingFrequency > 0) {
      logger.debug(
        `Starting ping interval with frequency ${this._pingFrequency}ms and timeout ${this._pingTimeout}ms`
      );
      this._pingInterval = setInterval(() => this.sendPing(), this._pingFrequency);
    }
  }

  private async sendPing(): Promise<void> {
    if (!this._isRunning) {
      return;
    }

    try {
      const pingId = `ping-${Date.now()}`;
      const pingRequest: JSONRPCMessage = {
        jsonrpc: '2.0' as const,
        id: pingId,
        method: 'ping',
      };

      logger.debug(`Sending ping request: ${JSON.stringify(pingRequest)}`);

      const timeoutId = setTimeout(() => {
        logger.warn(
          `Ping ${pingId} timed out after ${this._pingTimeout}ms - connection may be stale`
        );
        this._pingTimeouts.delete(pingId);

        this._onerror?.(new Error(`Ping timeout (${pingId}) - connection may be stale`));
      }, this._pingTimeout);

      this._pingTimeouts.set(pingId, timeoutId);

      await this.send(pingRequest);
    } catch (error) {
      logger.error(`Error sending ping: ${error}`);
    }
  }

  private handlePingMessage(message: JSONRPCMessage): boolean {
    if ('method' in message && message.method === 'ping') {
      const id = 'id' in message ? message.id : undefined;
      logger.debug(`Received ping request: ${JSON.stringify(message)}`);

      if (id !== undefined) {
        const response = {
          jsonrpc: '2.0' as const,
          id: id,
          result: {},
        };
        logger.debug(`Sending ping response: ${JSON.stringify(response)}`);

        this.send(response).catch((error) => logger.error(`Error responding to ping: ${error}`));
      }

      return true;
    }

    if (
      'id' in message &&
      message.id &&
      typeof message.id === 'string' &&
      message.id.startsWith('ping-') &&
      'result' in message
    ) {
      logger.debug(`Received ping response: ${JSON.stringify(message)}`);

      const timeoutId = this._pingTimeouts.get(message.id);
      if (timeoutId) {
        clearTimeout(timeoutId);
        this._pingTimeouts.delete(message.id);
        logger.debug(`Cleared timeout for ping response: ${message.id}`);
      }

      return true;
    }

    return false;
  }

  async handleRequest(req: IncomingMessage, res: ServerResponse, body?: any): Promise<void> {
    return this._sdkTransport.handleRequest(req, res, body);
  }

  async send(message: JSONRPCMessage): Promise<void> {
    await this._sdkTransport.send(message);
  }

  async close(): Promise<void> {
    if (!this._isRunning) {
      return;
    }

    this._isRunning = false;
    this._sessionInitialized = false;

    if (this._pingInterval) {
      clearInterval(this._pingInterval);
      this._pingInterval = undefined;
    }

    for (const timeoutId of this._pingTimeouts.values()) {
      clearTimeout(timeoutId);
    }
    this._pingTimeouts.clear();

    await this._sdkTransport.close();

    return new Promise((resolve) => {
      if (!this._server) {
        resolve();
        return;
      }

      this._server.close(() => {
        logger.info('HTTP server stopped');
        this._server = undefined;
        resolve();
      });
    });
  }

  isRunning(): boolean {
    return this._isRunning && !!this._server;
  }
}
