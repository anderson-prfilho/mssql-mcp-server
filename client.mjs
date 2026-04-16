// MCP Client implementation for MS SQL Server
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import readline from 'readline';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import util from 'util';
import { spawn } from 'child_process';

// Load environment variables
dotenv.config();

// Get current directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Create command-line interface
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

// Client configuration
const clientConfig = {
    name: "MSSQL-MCP-Client",
    version: "1.0.0"
};

// Capabilities configuration
const capabilities = {
    capabilities: {
        resources: {},
        tools: {},
        prompts: {}
    }
};

// Create client
const client = new Client(clientConfig, capabilities);

// Debug logging utility
function debugLog(message, obj) {
    if (process.env.DEBUG === 'true') {
        console.log(message);
        if (obj) {
            console.log(util.inspect(obj, { depth: 3, colors: true }));
        }
    }
}

// Helper to print formatted results with better formatting
function printResult(result) {
    console.log('\n📋 Result:');
    
    if (result.isError) {
        console.log('❌ Error returned from server:');
    }
    
    if (Array.isArray(result.content)) {
        result.content.forEach(item => {
            if (item.type === 'text') {
                console.log('📝 Text response:');
                console.log('-------------------------------------------');
                console.log(item.text);
                console.log('-------------------------------------------');
            } else {
                console.log(`📄 ${item.type} response:`);
                console.log(item);
            }
        });
    } else {
        console.log('⚠️ Unexpected response format:', result);
    }
}

// Enhance client methods with better logging and error handling
function enhanceClientMethods() {
    // Enhance readResource method
    const originalReadResource = client.readResource.bind(client);
    client.readResource = async function(uri) {
        console.log(`📤 Sending request: readResource ${uri}`);
        try {
            // MCP SDK expects an object with uri property, not a string
            const params = typeof uri === 'string' ? { uri } : uri;
            const result = await originalReadResource(params);
            debugLog(`📥 Received response: success`, result);
            return result;
        } catch (err) {
            console.error(`❌ Error in readResource:`, err);
            throw err;
        }
    };
    
    // Enhance callTool method
    const originalCallTool = client.callTool.bind(client);
    client.callTool = async function(name, args) {
        console.log(`📤 Sending request: callTool ${name}`);
        debugLog(`   Arguments:`, args);
        try {
            // MCP SDK expects { name, arguments } object, not separate arguments
            const params = { name, arguments: args || {} };
            const result = await originalCallTool(params);
            debugLog(`📥 Received response: success`, result);
            return result;
        } catch (err) {
            console.error(`❌ Error in callTool:`, err);
            throw err;
        }
    };
    
    // Enhance listResources method
    const originalListResources = client.listResources.bind(client);
    client.listResources = async function() {
        console.log(`📤 Sending request: listResources`);
        try {
            const result = await originalListResources();
            debugLog(`📥 Received response: success`, result);
            return result;
        } catch (err) {
            console.error(`❌ Error in listResources:`, err);
            throw err;
        }
    };
    
    // Enhance listTools method
    const originalListTools = client.listTools.bind(client);
    client.listTools = async function() {
        console.log(`📤 Sending request: listTools`);
        try {
            const result = await originalListTools();
            debugLog(`📥 Received response: success`, result);
            return result;
        } catch (err) {
            console.error(`❌ Error in listTools:`, err);
            throw err;
        }
    };
    
    // Enhance listPrompts method
    const originalListPrompts = client.listPrompts.bind(client);
    client.listPrompts = async function() {
        console.log(`📤 Sending request: listPrompts`);
        try {
            const result = await originalListPrompts();
            debugLog(`📥 Received response: success`, result);
            return result;
        } catch (err) {
            console.error(`❌ Error in listPrompts:`, err);
            throw err;
        }
    };
    
    // Enhance getPrompt method
    const originalGetPrompt = client.getPrompt.bind(client);
    client.getPrompt = async function(name, args) {
        console.log(`📤 Sending request: getPrompt ${name}`);
        debugLog(`   Arguments:`, args);
        try {
            // MCP SDK expects { name, arguments } object
            const params = { name, arguments: args || {} };
            const result = await originalGetPrompt(params);
            debugLog(`📥 Received response: success`, result);
            return result;
        } catch (err) {
            console.error(`❌ Error in getPrompt:`, err);
            throw err;
        }
    };
}

// Connect to the MCP server
async function connectToServer() {
    console.log('\n=======================================');
    console.log('      🔍 MSSQL MCP CLIENT 🔍');
    console.log('=======================================');
    console.log('🚀 Starting MSSQL MCP Client...');
    
    // Create appropriate transport based on environment variable
    const transportType = process.env.TRANSPORT || 'stdio';
    console.log(`🔄 Connecting to MCP server using ${transportType} transport...`);
    
    let transport;
    
    // Create appropriate transport
    if (transportType === 'stdio') {
        // For stdio transport, start the server as a child process
        transport = new StdioClientTransport();
    } else if (transportType === 'sse') {
        // For SSE transport, we need to connect to a server that's already running
        
        // Ensure the URL is properly formed
        let serverUrl = process.env.SERVER_URL || 'http://localhost:3333';
        
        // Make sure serverUrl doesn't have a trailing slash
        if (serverUrl.endsWith('/')) {
            serverUrl = serverUrl.slice(0, -1);
        }
        
        // Verify that the server is running separately before trying to connect
        console.log(`🌐 Connecting to server at ${serverUrl}`);
        console.log(`   Make sure the server is running with: TRANSPORT=sse npm start`);
        console.log(`   The server should be listening on port ${new URL(serverUrl).port}`);
        
        // Create SSE transport with properly formatted URLs
        const sseEndpoint = `${serverUrl}/sse`;
        const messagesEndpoint = `${serverUrl}/messages`;
        
        console.log(`   - SSE endpoint: ${sseEndpoint}`);
        console.log(`   - Messages endpoint: ${messagesEndpoint}`);
        
        try {
            // The SSEClientTransport constructor expects the URL of the SSE endpoint
            // It will automatically receive the messages endpoint when connecting
            transport = new SSEClientTransport(new URL(sseEndpoint));
            
            console.log('   Created SSE transport successfully');
            
            // Add error handler to transport for better debugging
            transport.onerror = (error) => {
                console.error(`❌ SSE Transport error:`, error);
            };
        } catch (err) {
            console.error(`   ❌ Error creating SSE transport: ${err.message}`);
            console.error(`   ${err.stack}`);
            throw err;
        }
    } else {
        throw new Error(`Unknown transport type: ${transportType}`);
    }

    // Enhance client methods to add logging
    enhanceClientMethods();

    // Connect to the transport
    try {
        console.log('   Attempting to connect to transport...');
        await client.connect(transport);
        console.log(`> ✅ Connected to MCP server using ${transportType} transport`);
        console.log(`🔌 Client connection established`);
    } catch (err) {
        console.error(`❌ Connection error: ${err.message}`);
        console.error(`   If using SSE transport, make sure the server is running separately with TRANSPORT=sse npm start`);
        
        // Additional debugging for SSE transport errors
        if (err.stack) {
            console.error(err.stack);
        }
        process.exit(1);
    }
}

// List available resources
async function listResources() {
    try {
        console.log('\n🔍 Listing available resources...');
        
        const result = await client.listResources();
        
        console.log('\n✅ Resources retrieved successfully!');
        console.log('\n📋 Available Resources:');
        console.log('-------------------------------------------');
        
        // The MCP SDK returns { resources: [...] }, not an array directly
        const resources = result.resources || result || [];
        
        if (!Array.isArray(resources) || resources.length === 0) {
            console.log('No resources available.');
        } else {
            resources.forEach(resource => {
                console.log(`📑 ${resource.name}: ${resource.description || 'No description'}`);
                console.log(`   URI: ${resource.uri}`);
                console.log();
            });
        }
        
        console.log('-------------------------------------------');
    } catch (err) {
        console.error('❌ Error listing resources:', err);
    }
}

// List available tools
async function listTools() {
    try {
        console.log('\n🔍 Listing available tools...');
        
        const result = await client.listTools();
        
        console.log('\n✅ Tools retrieved successfully!');
        console.log('\n📋 Available Tools:');
        console.log('-------------------------------------------');
        
        // The MCP SDK returns { tools: [...] }, not an array directly
        const tools = result.tools || result || [];
        
        if (!Array.isArray(tools) || tools.length === 0) {
            console.log('No tools available.');
        } else {
            tools.forEach(tool => {
                console.log(`🔧 ${tool.name}: ${tool.description || 'No description'}`);
                console.log();
            });
        }
        
        console.log('-------------------------------------------');
    } catch (err) {
        console.error('❌ Error listing tools:', err);
    }
}

// List available prompts
async function listPrompts() {
    try {
        console.log('\n🔍 Listing available prompts...');
        
        const result = await client.listPrompts();
        
        console.log('\n✅ Prompts retrieved successfully!');
        console.log('\n📋 Available Prompts:');
        console.log('-------------------------------------------');
        
        // The MCP SDK returns { prompts: [...] }, not an array directly
        const prompts = result.prompts || result || [];
        
        if (!Array.isArray(prompts) || prompts.length === 0) {
            console.log('No prompts available.');
        } else {
            prompts.forEach(prompt => {
                console.log(`📝 ${prompt.name}: ${prompt.description || 'No description'}`);
                console.log();
            });
        }
        
        console.log('-------------------------------------------');
    } catch (err) {
        console.error('❌ Error listing prompts:', err);
    }
}

// Read database schema through resource
async function readSchema() {
    try {
        console.log('\n🔍 Reading database schema...');
        
        const schema = await client.readResource('schema://database');
        
        console.log('\n✅ Database schema retrieved successfully!');
        console.log('\n📋 Database Schema:');
        console.log('-------------------------------------------');
        
        schema.contents.forEach(content => {
            console.log(content.text);
        });
        
        console.log('-------------------------------------------');
    } catch (err) {
        console.error('❌ Error reading schema:', err);
    }
}

// Read database tables list through resource
async function readTablesList() {
    try {
        console.log('\n🔍 Reading tables list...');
        
        // Add a more detailed debug message 
        console.log('📤 Sending request: readResource tables://list');
        
        try {
            const tables = await client.readResource('tables://list');
            
            console.log('\n✅ Database tables list retrieved successfully!');
            console.log('\n📋 Database Tables:');
            console.log('-------------------------------------------');
            
            if (!tables || !tables.contents || !Array.isArray(tables.contents)) {
                console.error('❌ Unexpected response format:', tables);
                return;
            }
            
            tables.contents.forEach(content => {
                console.log(content.text);
            });
            
            console.log('-------------------------------------------');
        } catch (readError) {
            console.error('❌ Error in readResource:', readError);
            
            // Add more detailed error logging
            if (readError.cause) {
                console.error('Caused by:', readError.cause);
            }
            
            if (readError.message && readError.message.includes('POSTing to endpoint')) {
                console.error('\n⚠️ Server may have responded with an invalid JSON-RPC response');
                console.error('Please check server logs for details');
            }
            
            throw readError;
        }
    } catch (err) {
        console.error('❌ Error reading tables list:', err);
    }
}

// Generate SQL query through prompt
async function generateQuery(description, tables = []) {
    try {
        console.log('\n🔍 Generating SQL query...');
        console.log(`Description: ${description}`);
        if (tables.length > 0) {
            console.log(`Tables: ${tables.join(', ')}`);
        }
        
        // Get prompt from server
        // MCP prompt arguments must be strings, so convert array to comma-separated string
        const result = await client.getPrompt('generate-query', {
            description,
            tables: tables.length > 0 ? tables.join(',') : undefined
        });
        
        console.log('\n✅ Prompt retrieved successfully!');
        console.log('\n📝 Generated Prompt (for LLM execution):');
        console.log('-------------------------------------------');
        
        // Display the prompt messages
        const messages = result.messages || [];
        messages.forEach((msg, idx) => {
            console.log(`\n[${msg.role}]:`);
            if (msg.content?.text) {
                console.log(msg.content.text);
            } else if (typeof msg.content === 'string') {
                console.log(msg.content);
            } else {
                console.log(JSON.stringify(msg.content, null, 2));
            }
        });
        
        console.log('\n-------------------------------------------');
        console.log('💡 Note: This prompt should be sent to an LLM (like Claude) to generate the actual SQL query.');
    } catch (err) {
        console.error('❌ Error generating query:', err);
    }
}

// Execute SQL query through tool
async function executeQuery(sql) {
    try {
        console.log('\n🔍 Executing SQL query...');
        console.log(`Query: ${sql}`);
        
        const result = await client.callTool('mcp_execute_query', { sql });
        
        console.log('\n✅ SQL query executed successfully!');
        printResult(result);
    } catch (err) {
        console.error('❌ Error executing query:', err);
    }
}

// Get table details
async function getTableDetails(tableName) {
    try {
        console.log(`\n🔍 Getting details for table: ${tableName}...`);
        
        const result = await client.callTool('mcp_table_details', { tableName });
        
        console.log('\n✅ Table details retrieved successfully!');
        printResult(result);
    } catch (err) {
        console.error('❌ Error getting table details:', err);
    }
}

// Display menu and handle user interaction
function showMenu() {
    console.log('\n======================================');
    console.log('     🔍 MSSQL MCP CLIENT MENU 🔍');
    console.log('======================================');
    console.log('1. 📋 List available resources');
    console.log('2. 🔧 List available tools');
    console.log('3. 📝 List available prompts');
    console.log('4. 🔍 Execute SQL query');
    console.log('5. 📊 Get table details');
    console.log('6. 📑 Read database schema');
    console.log('7. 📑 Read tables list');
    console.log('8. 📝 Generate SQL query');
    console.log('9. 🚪 Exit');
    console.log('======================================');
    
    rl.question('\n🔍 Select an option (1-9): ', async (answer) => {
        switch (answer) {
            case '1':
                await listResources();
                showMenu();
                break;
            case '2':
                await listTools();
                showMenu();
                break;
            case '3':
                await listPrompts();
                showMenu();
                break;
            case '4':
                rl.question('✏️ Enter SQL query: ', async (sql) => {
                    await executeQuery(sql);
                    showMenu();
                });
                break;
            case '5':
                rl.question('✏️ Enter table name: ', async (tableName) => {
                    await getTableDetails(tableName);
                    showMenu();
                });
                break;
            case '6':
                await readSchema();
                showMenu();
                break;
            case '7':
                await readTablesList();
                showMenu();
                break;
            case '8':
                rl.question('✏️ Enter query description: ', async (description) => {
                    rl.question('✏️ Enter tables (comma-separated) or leave empty: ', async (tablesInput) => {
                        const tables = tablesInput ? tablesInput.split(',').map(t => t.trim()) : [];
                        await generateQuery(description, tables);
                        showMenu();
                    });
                });
                break;
            case '9':
                console.log('👋 Goodbye!');
                rl.close();
                process.exit(0);
                break;
            default:
                console.log('❌ Invalid option. Please try again.');
                showMenu();
                break;
        }
    });
}

// Main entry point
async function main() {
    try {
        // Connect to server
        await connectToServer();
        
        // Show interactive menu
        showMenu();
    } catch (err) {
        console.error('❌ Error starting client:', err);
        process.exit(1);
    }
}

// Start the client
main(); 