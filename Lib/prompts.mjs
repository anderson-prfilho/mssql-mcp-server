// lib/prompts.js - Prompt implementations
import { z } from 'zod';
import { logger } from './logger.mjs';

/**
 * Register all prompts with the MCP server
 * @param {object} server - MCP server instance
 */
export function registerPrompts(server) {
    logger.info('Registering prompts');
    
    // Register prompts
    registerGenerateQueryPrompt(server);
    registerExplainQueryPrompt(server);
    registerCreateTablePrompt(server);
    
    logger.info('Prompts registered successfully');
}

/**
 * Register the generate-query prompt
 * @param {object} server - MCP server instance
 */
function registerGenerateQueryPrompt(server) {
    server.prompt(
        "generate-query",
        { 
            description: z.string().min(1, "Description cannot be empty"),
            tables: z.string().optional(),  // Comma-separated table names
            limit: z.string().optional()    // MCP prompt args are strings
        },
        ({ description, tables, limit }) => {
            // Parse tables from comma-separated string
            const tablesList = tables ? tables.split(',').map(t => t.trim()).filter(t => t) : [];
            const limitNum = limit ? parseInt(limit, 10) : 100;
            
            // Build a prompt that helps generate a SQL query
            const tablesContext = tablesList.length > 0 
                ? `The query should involve these tables: ${tablesList.join(', ')}.` 
                : 'Use appropriate tables from the database.';
            
            return {
                messages: [{
                    role: "user",
                    content: {
                        type: "text",
                        text: `Please help me write a SQL query for Microsoft SQL Server that ${description}. ${tablesContext}

Key requirements:
1. Use TOP ${limitNum} for safety or appropriate pagination
2. Include clear column names (avoid SELECT *)
3. Add proper JOIN conditions if multiple tables are used
4. Include informative column aliases for complex expressions
5. Add comments for clarity

Please provide just the SQL query without explanations.`
                    }
                }]
            };
        }
    );
}

/**
 * Register the explain-query prompt
 * @param {object} server - MCP server instance
 */
function registerExplainQueryPrompt(server) {
    server.prompt(
        "explain-query",
        { 
            query: z.string().min(1, "Query cannot be empty")
        },
        ({ query }) => {
            return {
                messages: [{
                    role: "user",
                    content: {
                        type: "text",
                        text: `Please explain this SQL Server query in detail:

\`\`\`sql
${query}
\`\`\`

Please provide:
1. A plain language explanation of what this query does
2. Breakdown of each clause and operation
3. Description of any JOINs and their relationships
4. Explanation of filters, grouping, and aggregations
5. Performance considerations or potential optimizations
6. Any potential issues or edge cases to be aware of`
                    }
                }]
            };
        }
    );
}

/**
 * Register the create-table prompt
 * @param {object} server - MCP server instance
 */
function registerCreateTablePrompt(server) {
    server.prompt(
        "create-table",
        {
            tableName: z.string().min(1, "Table name cannot be empty"),
            description: z.string().min(1, "Description cannot be empty"),
            schema: z.string().optional().default("dbo")
        },
        ({ tableName, description, schema = "dbo" }) => {
            return {
                messages: [{
                    role: "user",
                    content: {
                        type: "text",
                        text: `Please help me create a SQL Server CREATE TABLE statement for a table called ${schema}.${tableName} based on this description:

${description}

The CREATE TABLE statement should:
1. Include appropriate column names and data types
2. Define appropriate primary keys
3. Include foreign key constraints if needed
4. Add appropriate indexes
5. Include NULL/NOT NULL constraints
6. Add appropriate default values
7. Include check constraints if appropriate
8. Follow SQL Server best practices

Please provide just the CREATE TABLE statement without explanations.`
                    }
                }]
            };
        }
    );
}