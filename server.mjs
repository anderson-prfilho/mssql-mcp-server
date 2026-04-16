// server.js - Main MCP Server Implementation
import dotenv from 'dotenv';
import express from 'express';
import bodyParser from 'body-parser';
import sql from 'mssql';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { z } from 'zod';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import crypto from 'crypto';
import cors from 'cors';
import helmet from 'helmet';
import { rateLimit } from 'express-rate-limit';

// Import database utilities
import { initializeDbPool, executeQuery, getDbConfig } from './Lib/database.mjs';

// Import tool implementations
import { registerDatabaseTools } from './Lib/tools.mjs';

// Import resource implementations
import { registerDatabaseResources } from './Lib/resources.mjs';

// Import prompt implementations
import { registerPrompts } from './Lib/prompts.mjs';

// Import utilities
import { logger } from './Lib/logger.mjs';
import { getReadableErrorMessage, createJsonRpcError } from './Lib/errors.mjs';

// Load environment variables
dotenv.config();

/** MCP stdio: stderr com texto que não seja JSON-RPC quebra o cliente; DEP0123 (TLS + IP) é comum com mssql. */
if ((process.env.TRANSPORT || 'stdio') === 'stdio' && typeof process.emitWarning === 'function') {
    const origEmitWarning = process.emitWarning.bind(process);
    process.emitWarning = function (...args) {
        const code = args[2];
        const msg = typeof args[0] === 'string' ? args[0] : args[0]?.message;
        if (code === 'DEP0123' || (msg && String(msg).includes('TLS ServerName'))) {
            return;
        }
        return origEmitWarning(...args);
    };
}

// Get the directory name
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const PORT = process.env.PORT || 3333;
const TRANSPORT = process.env.TRANSPORT || 'stdio';
const HOST = process.env.HOST || '0.0.0.0';
const QUERY_RESULTS_PATH = process.env.QUERY_RESULTS_PATH || path.join(__dirname, 'query_results');
const PING_INTERVAL = parseInt(process.env.PING_INTERVAL) || 15000; // Ping every 15 seconds by default
const SERVER_VERSION = '1.1.0';

// Create results directory if it doesn't exist
if (!fs.existsSync(QUERY_RESULTS_PATH)) {
    fs.mkdirSync(QUERY_RESULTS_PATH, { recursive: true });
    logger.info(`Created results directory: ${QUERY_RESULTS_PATH}`);
}

// Create Express app to handle HTTP requests for SSE transport
const app = express();
const httpServer = http.createServer(app);

// Prevent Node.js from closing long-lived SSE connections
httpServer.keepAliveTimeout = 0;
httpServer.headersTimeout = 0;

// Security middleware
app.use(helmet({ contentSecurityPolicy: false })); // Modified helmet config for SSE
app.use(cors());

// Apply bodyParser.json() to all routes EXCEPT /messages
// The /messages route needs raw stream for SSEServerTransport.handlePostMessage()
app.use((req, res, next) => {
    if (req.path === '/messages') {
        return next();
    }
    bodyParser.json({ limit: '10mb' })(req, res, next);
});

// Rate limiting
const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 300, // Limit each IP to 300 requests per minute (multiple sessions + frequent pings)
    standardHeaders: true,
    legacyHeaders: false,
    message: {
        status: 429,
        message: 'Too many requests, please try again later.'
    }
});
app.use(limiter);

// Logging middleware -- skip high-frequency SSE message posts to avoid log bloat
app.use((req, res, next) => {
    if (req.method === 'POST' && req.path === '/messages') {
        logger.debug(`${req.method} ${req.url}`);
    } else {
        logger.info(`${req.method} ${req.url}`);
    }
    next();
});

// Error handling middleware
app.use((err, req, res, next) => {
    logger.error(`Express error: ${err.message}`);
    res.status(500).json({
        jsonrpc: "2.0",
        error: createJsonRpcError(-32603, `Internal error: ${err.message}`)
    });
});

// Create MCP server instance (capabilities belong in the second argument per @modelcontextprotocol/sdk)
const server = new McpServer(
    { name: 'MSSQL-MCP-Server', version: SERVER_VERSION },
    {
        capabilities: {
            resources: { listChanged: true },
            tools: { listChanged: true },
            prompts: { listChanged: true }
        }
    }
);

// Make sure server._tools exists
if (!server._tools) {
    server._tools = {};
}

// Make sure server._resources exists for tracking registered resources
if (!server._resources) {
    server._resources = {};
}

// Add a helper method to the server to execute tools directly
server.executeToolCall = async function (toolName, args) {
    // Find the tool in the registered tools
    logger.info(`Looking for tool: ${toolName}`);
    const tool = this._tools ? this._tools[toolName] : null;

    if (!tool) {
        const availableTools = Object.keys(this._tools || {}).join(', ');
        logger.error(`Tool ${toolName} not found. Available tools: ${availableTools}`);
        throw new Error(`Tool ${toolName} not found. Available tools: ${availableTools.length > 100 ? availableTools.substring(0, 100) + '...' : availableTools}`);
    }

    try {
        logger.info(`Executing tool ${toolName} directly with args: ${JSON.stringify(args)}`);
        const result = await tool.handler(args);
        logger.info(`Tool ${toolName} executed successfully`);
        return result;
    } catch (err) {
        logger.error(`Error executing tool ${toolName}: ${err.message}`);
        throw err;
    }
};

// IMPORTANT: Register database tools BEFORE setting up HTTP routes
try {
    // Register database tools (execute-query, table-details, etc.)
    logger.info("Registering database tools...");
    registerDatabaseTools(server);

    // Register database resources (tables, schema, views, etc.)
    logger.info("Registering database resources...");
    registerDatabaseResources(server);

    // Register prompts (generate-query, etc.)
    logger.info("Registering prompts...");
    registerPrompts(server);

    // Debug log for tools
    const registeredTools = Object.keys(server._tools || {});
    logger.info(`Registered tools (${registeredTools.length}): ${registeredTools.join(', ')}`);
} catch (error) {
    logger.error(`Failed to register tools: ${error.message}`);
    logger.error(error.stack);
}

// Transport variables - supports multiple simultaneous SSE connections
const sseConnections = new Map(); // sessionId -> { transport, server, res, pingIntervalId }

function createMcpServerInstance() {
    const instance = new McpServer(
        { name: 'MSSQL-MCP-Server', version: SERVER_VERSION },
        {
            capabilities: {
                resources: { listChanged: true },
                tools: { listChanged: true },
                prompts: { listChanged: true }
            }
        }
    );

    try {
        registerDatabaseTools(instance);
        registerDatabaseResources(instance);
        registerPrompts(instance);
    } catch (error) {
        logger.error(`Failed to register tools on new instance: ${error.message}`);
    }

    return instance;
}

function cleanupSession(sessionId) {
    const session = sseConnections.get(sessionId);
    if (!session) return;

    logger.info(`Cleaning up SSE session ${sessionId}`);

    if (session.pingIntervalId) {
        clearInterval(session.pingIntervalId);
    }

    try {
        if (session.res && !session.res.writableEnded) {
            session.res.end();
        }
    } catch (err) {
        logger.warn(`Error ending response for session ${sessionId}: ${err.message}`);
    }

    sseConnections.delete(sessionId);
    logger.info(`Session ${sessionId} cleaned up. Active sessions: ${sseConnections.size}`);
}

// Add HTTP server status endpoint
app.get('/', (req, res) => {
    const dbConfig = getDbConfig(true); // Get sanitized config (no password)

    res.status(200).json({
        status: 'ok',
        message: 'MCP Server is running',
        transport: TRANSPORT,
        endpoints: {
            sse: '/sse',
            messages: '/messages',
            diagnostics: '/diagnostic',
            query_results: {
                list: '/query-results',
                detail: '/query-results/:uuid'
            }
        },
        connection_info: {
            ping_interval_ms: PING_INTERVAL,
            active_sessions: sseConnections.size
        },
        database_info: {
            server: dbConfig.server,
            database: dbConfig.database,
            user: dbConfig.user
        },
        version: SERVER_VERSION
    });
});

// Add an endpoint to list all tools
app.get('/tools', (req, res) => {
    try {
        // Access tools directly from the server instance
        const tools = server._tools || {};

        const toolList = Object.keys(tools).map(name => {
            return {
                name,
                schema: tools[name].schema,
                source: 'internal'
            };
        });

        logger.info(`Tool listing requested. Found ${toolList.length} tools.`);
        logger.info(`Tools from internal: ${Object.keys(tools).join(', ')}`);

        res.status(200).json({
            count: toolList.length,
            tools: toolList,
            debug: {
                internalToolKeys: Object.keys(tools)
            }
        });
    } catch (error) {
        logger.error(`Error listing tools: ${error.message}`);
        res.status(500).json({
            error: `Failed to list tools: ${error.message}`,
            stack: error.stack
        });
    }
});

// Diagnostic endpoint 
app.get('/diagnostic', async (req, res) => {
    try {
        const dbConfig = getDbConfig(true); // Get sanitized config (no password)

        const diagnosticInfo = {
            status: 'ok',
            server: {
                version: process.version,
                platform: process.platform,
                arch: process.arch,
                uptime: process.uptime()
            },
            mcp: {
                transport: TRANSPORT,
                activeSessions: sseConnections.size,
                sessionIds: Array.from(sseConnections.keys()),
                version: SERVER_VERSION,
                pingIntervalMs: PING_INTERVAL
            },
            database: {
                server: dbConfig.server,
                database: dbConfig.database,
                user: dbConfig.user,
                port: dbConfig.port
            },
            endpoints: {
                sse: `${req.protocol}://${req.get('host')}/sse`,
                messages: `${req.protocol}://${req.get('host')}/messages`,
                queryResults: `${req.protocol}://${req.get('host')}/query-results`
            }
        };

        // Test database connection
        try {
            await executeQuery('SELECT 1 AS TestConnection');
            diagnosticInfo.database.connectionTest = 'successful';
        } catch (err) {
            diagnosticInfo.database.connectionTest = 'failed';
            diagnosticInfo.database.connectionError = err.message;
        }

        res.status(200).json(diagnosticInfo);
    } catch (error) {
        logger.error(`Diagnostic error: ${error.message}`);
        res.status(500).json({
            status: 'error',
            message: error.message
        });
    }
});

// Direct cursor guide endpoint
app.get('/cursor-guide', (req, res) => {
    // Comprehensive guide for cursor-based pagination
    const guideText = `
# SQL Cursor-Based Pagination Guide

Cursor-based pagination is an efficient approach for paginating through large datasets, especially when:
- You need stable pagination through frequently changing data
- You're handling very large datasets where OFFSET/LIMIT becomes inefficient
- You want better performance for deep pagination

## Key Concepts

1. **Cursor**: A pointer to a specific item in a dataset, typically based on a unique, indexed field
2. **Direction**: You can paginate forward (next) or backward (previous)
3. **Page Size**: The number of items to return per request

## Example Usage

Using cursor-based pagination with our SQL tools:

\`\`\`javascript
// First page (no cursor)
const firstPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at"
});

// Next page (using cursor from previous response)
const nextPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at",
  cursor: firstPage.result.pagination.nextCursor,
  direction: "next"
});

// Previous page (going back)
const prevPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at",
  cursor: nextPage.result.pagination.prevCursor,
  direction: "prev"
});
\`\`\`

## Best Practices

1. **Choose an appropriate cursor field**:
   - Should be unique or nearly unique (ideally indexed)
   - Common choices: timestamps, auto-incrementing IDs
   - Compound cursors can be used for non-unique fields (e.g., "timestamp:id")

2. **Order matters**:
   - Always include an ORDER BY clause that includes your cursor field
   - Consistent ordering is essential (always ASC or always DESC)

3. **Handle edge cases**:
   - First/last page detection
   - Empty result sets
   - Missing or invalid cursors

4. **Performance considerations**:
   - Use indexed fields for cursors
   - Avoid expensive joins in paginated queries
   - Consider caching results for frequently accessed pages
`;

    // Send both JSON and plain text formats
    if (req.headers.accept && req.headers.accept.includes('application/json')) {
        res.status(200).json({
            jsonrpc: "2.0",
            result: {
                content: [{
                    type: "text",
                    text: guideText
                }]
            }
        });
    } else {
        res.status(200).type('text/markdown').send(guideText);
    }
});

// SSE endpoint for client to connect
app.get('/sse', async (req, res) => {
    logger.info('New SSE connection request received');

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');

    try {
        const sessionServer = createMcpServerInstance();
        const transport = new SSEServerTransport('/messages', res);

        // The SDK generates its own sessionId -- use it as our Map key
        const sessionId = transport.sessionId;
        logger.info(`Creating SSE transport for session ${sessionId}`);

        transport.onmessage = function (message) {
            logger.debug(`[${sessionId}] Transport received message`);
        };

        transport.onerror = function (error) {
            logger.error(`[${sessionId}] Transport error: ${error}`);
        };

        transport.onclose = function () {
            logger.info(`[${sessionId}] Transport closed`);
            cleanupSession(sessionId);
        };

        await sessionServer.connect(transport);

        const pingIntervalId = setInterval(() => {
            if (res && !res.writableEnded) {
                res.write('event: ping\n');
                res.write(`data: ${Date.now()}\n\n`);
            } else {
                cleanupSession(sessionId);
            }
        }, PING_INTERVAL);

        sseConnections.set(sessionId, { transport, server: sessionServer, res, pingIntervalId });
        logger.info(`SSE session ${sessionId} connected. Active sessions: ${sseConnections.size}`);

        req.on('close', () => {
            logger.info(`SSE client disconnected (session: ${sessionId})`);
            cleanupSession(sessionId);
        });

    } catch (error) {
        logger.error(`Failed to set up SSE transport: ${error.message}`);
        if (!res.headersSent) {
            res.status(500).end(`Error: ${error.message}`);
        }
    }
});

// Messages endpoint for client to send messages
// IMPORTANT: This route does NOT use bodyParser - the SDK reads the stream directly
app.post('/messages', async (req, res) => {
    const sessionId = req.query.sessionId;

    if (!sessionId) {
        // Fallback: try to find any active session (backward compatibility)
        if (sseConnections.size === 0) {
            logger.error('No SSE sessions available');
            return res.status(503).json({
                jsonrpc: "2.0", id: null,
                error: { code: -32000, message: "No active SSE sessions. Connect to /sse endpoint first." }
            });
        }
        // Use the most recent session
        const fallbackId = Array.from(sseConnections.keys()).pop();
        logger.warn(`No sessionId in request, falling back to session ${fallbackId}`);
        const session = sseConnections.get(fallbackId);
        try {
            await session.transport.handlePostMessage(req, res);
        } catch (error) {
            logger.error(`Error processing message (fallback): ${error.message}`);
            if (!res.headersSent) {
                return res.status(500).json({
                    jsonrpc: "2.0", id: null,
                    error: { code: -32603, message: "Internal server error: " + error.message }
                });
            }
        }
        return;
    }

    const session = sseConnections.get(sessionId);
    if (!session) {
        logger.error(`Session ${sessionId} not found. Active sessions: ${Array.from(sseConnections.keys()).join(', ')}`);
        return res.status(503).json({
            jsonrpc: "2.0", id: null,
            error: { code: -32000, message: `Session ${sessionId} not found. Reconnect to /sse endpoint.` }
        });
    }

    try {
        await session.transport.handlePostMessage(req, res);
    } catch (error) {
        logger.error(`[${sessionId}] Error processing message: ${error.message}`);
        if (!res.headersSent) {
            return res.status(500).json({
                jsonrpc: "2.0", id: null,
                error: { code: -32603, message: "Internal server error: " + error.message }
            });
        }
    }
});

// Add HTTP endpoints to list and retrieve saved query results
app.get('/query-results', (req, res) => {
    try {
        if (!fs.existsSync(QUERY_RESULTS_PATH)) {
            return res.status(200).json({ results: [] });
        }

        // Read all JSON files in the results directory
        const files = fs.readdirSync(QUERY_RESULTS_PATH)
            .filter(file => file.endsWith('.json'))
            .map(file => {
                try {
                    const filepath = path.join(QUERY_RESULTS_PATH, file);
                    const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
                    return {
                        uuid: data.metadata.uuid,
                        timestamp: data.metadata.timestamp,
                        query: data.metadata.query,
                        rowCount: data.metadata.rowCount,
                        filename: file
                    };
                } catch (err) {
                    logger.error(`Error reading file ${file}: ${err.message}`);
                    return {
                        uuid: file.replace('.json', ''),
                        error: 'Could not read file metadata'
                    };
                }
            })
            // Sort by timestamp (most recent first)
            .sort((a, b) => {
                if (!a.timestamp) return 1;
                if (!b.timestamp) return -1;
                return new Date(b.timestamp) - new Date(a.timestamp);
            });

        res.status(200).json({ results: files });
    } catch (err) {
        logger.error(`Error listing query results: ${err.message}`);
        res.status(500).json({ error: err.message });
    }
});

app.get('/query-results/:uuid', (req, res) => {
    const { uuid } = req.params;
    const filepath = path.join(QUERY_RESULTS_PATH, `${uuid}.json`);

    if (!fs.existsSync(filepath)) {
        return res.status(404).json({ error: `Result with UUID ${uuid} not found` });
    }

    try {
        const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
        res.status(200).json(data);
    } catch (err) {
        logger.error(`Error retrieving query result ${uuid}: ${err.message}`);
        res.status(500).json({ error: err.message });
    }
});

// Add a debugging endpoint to directly register the cursor guide tool
app.get('/debug/register-cursor-guide', (req, res) => {
    try {
        logger.info('Manually registering cursor guide tool');

        // Create cursor guide tool schema and handler
        const cursorGuideSchema = {
            random_string: z.string().optional().describe("Dummy parameter for no-parameter tools")
        };

        const cursorGuideHandler = async (args) => {
            // Comprehensive guide for cursor-based pagination
            const guideText = `
# SQL Cursor-Based Pagination Guide

Cursor-based pagination is an efficient approach for paginating through large datasets, especially when:
- You need stable pagination through frequently changing data
- You're handling very large datasets where OFFSET/LIMIT becomes inefficient
- You want better performance for deep pagination

## Key Concepts

1. **Cursor**: A pointer to a specific item in a dataset, typically based on a unique, indexed field
2. **Direction**: You can paginate forward (next) or backward (previous)
3. **Page Size**: The number of items to return per request

## Example Usage

Using cursor-based pagination with our SQL tools:

\`\`\`javascript
// First page (no cursor)
const firstPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at"
});

// Next page (using cursor from previous response)
const nextPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at",
  cursor: firstPage.result.pagination.nextCursor,
  direction: "next"
});

// Previous page (going back)
const prevPage = await tool.call("mcp_paginated_query", {
  sql: "SELECT id, name, created_at FROM users ORDER BY created_at DESC",
  pageSize: 20,
  cursorField: "created_at",
  cursor: nextPage.result.pagination.prevCursor,
  direction: "prev"
});
\`\`\`

## Best Practices

1. **Choose an appropriate cursor field**:
   - Should be unique or nearly unique (ideally indexed)
   - Common choices: timestamps, auto-incrementing IDs
   - Compound cursors can be used for non-unique fields (e.g., "timestamp:id")

2. **Order matters**:
   - Always include an ORDER BY clause that includes your cursor field
   - Consistent ordering is essential (always ASC or always DESC)

3. **Handle edge cases**:
   - First/last page detection
   - Empty result sets
   - Missing or invalid cursors

4. **Performance considerations**:
   - Use indexed fields for cursors
   - Avoid expensive joins in paginated queries
   - Consider caching results for frequently accessed pages
`;

            return {
                content: [{
                    type: "text",
                    text: guideText
                }]
            };
        };

        // Register with only mcp_ prefix for consistency
        server.tool("mcp_cursor_guide", cursorGuideSchema, cursorGuideHandler);

        // Make sure these are directly accessible in _tools
        if (!server._tools) server._tools = {};
        server._tools["mcp_cursor_guide"] = { schema: cursorGuideSchema, handler: cursorGuideHandler };

        const toolNames = Object.keys(server._tools || {});

        res.status(200).json({
            success: true,
            message: 'Cursor guide tool manually registered',
            tools: toolNames
        });
    } catch (error) {
        logger.error(`Error registering cursor guide tool: ${error.message}`);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Add a debugging endpoint to list all tools and their details
app.get('/debug-tools', (req, res) => {
    try {
        // Examine server._tools directly
        const toolKeys = Object.keys(server._tools || {});

        // Build detailed response
        const toolDetails = {};
        for (const key of toolKeys) {
            try {
                const tool = server._tools[key];
                toolDetails[key] = {
                    hasHandler: !!tool.handler,
                    handlerType: typeof tool.handler,
                    hasSchema: !!tool.schema,
                    schemaKeys: tool.schema ? Object.keys(tool.schema) : []
                };
            } catch (err) {
                toolDetails[key] = { error: err.message };
            }
        }

        res.status(200).json({
            toolCount: toolKeys.length,
            toolNames: toolKeys,
            toolDetails,
            raw: server._tools
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            stack: error.stack
        });
    }
});

// Add debugging endpoint to list all registered tools
app.get('/debug/tools', (req, res) => {
    try {
        const allTools = server._tools || {};
        const toolNames = Object.keys(allTools);

        // Group tools by their base name (without prefix)
        const toolsByBaseName = {};

        toolNames.forEach(name => {
            let baseName = name;

            // Remove known prefixes
            if (name.startsWith('mcp_SQL_')) {
                baseName = name.substring(8);
            } else if (name.startsWith('mcp_')) {
                baseName = name.substring(4);
            } else if (name.startsWith('SQL_')) {
                baseName = name.substring(4);
            }

            if (!toolsByBaseName[baseName]) {
                toolsByBaseName[baseName] = [];
            }

            toolsByBaseName[baseName].push(name);
        });

        res.status(200).json({
            totalTools: toolNames.length,
            toolNamesByGroup: toolsByBaseName,
            allToolNames: toolNames
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            stack: error.stack
        });
    }
});

// Setup and start server
async function startServer() {
    try {
        logger.info(`Starting MS SQL MCP Server v${SERVER_VERSION}...`);

        // Initialize database connection pool (soft fail: MCP client stays alive if SQL is down; tools fail until DB is reachable)
        const dbOk = await initializeDbPool({ softFail: true });
        if (!dbOk) {
            logger.warn('SQL Server not reachable at startup. Check DB_SERVER, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE in .env. Tools will retry on first use.');
        }

        // Select transport based on configuration
        if (TRANSPORT === 'sse') {
            logger.info(`Setting up SSE transport on port ${PORT}`);

            // Start HTTP server for SSE transport
            await new Promise((resolve, reject) => {
                httpServer.listen(PORT, HOST, () => {
                    logger.info(`HTTP server listening on port ${PORT} and host ${HOST}`);
                    logger.info(`SSE endpoint: http://${HOST}:${PORT}/sse`);
                    logger.info(`Messages endpoint: http://${HOST}:${PORT}/messages`);
                    resolve();
                });

                httpServer.on('error', (error) => {
                    logger.error(`Failed to start HTTP server: ${error.message}`);
                    reject(error);
                });
            });

            logger.info('Waiting for SSE client connection...');
        } else if (TRANSPORT === 'stdio') {
            logger.info('Setting up STDIO transport');

            // For stdio transport, we can set up and connect immediately
            const transport = new StdioServerTransport();
            await server.connect(transport);

            logger.info('STDIO transport ready');
        } else {
            throw new Error(`Unsupported transport type: ${TRANSPORT}`);
        }

        // Add graceful shutdown handler
        process.on('SIGINT', async () => {
            logger.info('Shutting down server gracefully...');

            // Close all SSE sessions
            if (sseConnections.size > 0) {
                logger.info(`Closing ${sseConnections.size} active SSE sessions`);
                for (const sessionId of Array.from(sseConnections.keys())) {
                    cleanupSession(sessionId);
                }
            }

            // Close HTTP server if it's running
            if (httpServer && httpServer.listening) {
                logger.info('Closing HTTP server');
                await new Promise(resolve => httpServer.close(resolve));
            }

            // Close database pool
            try {
                await sql.close();
                logger.info('Database connections closed');
            } catch (err) {
                logger.error(`Error closing database connections: ${err.message}`);
            }

            logger.info('Server shutdown complete');
            process.exit(0);
        });

        logger.info('MCP Server startup complete');
    } catch (err) {
        logger.error(`Failed to start MCP server: ${err.message}`);
        process.exit(1);
    }
}

// Start the server
startServer();

export { app, server, httpServer };