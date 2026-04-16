// lib/resources.js - Database resource implementations
import { executeQuery, sanitizeSqlIdentifier, formatSqlError } from './database.mjs';
import { logger } from './logger.mjs';
import { createJsonRpcError } from './errors.mjs';

/**
 * Register all database-related resources with the MCP server
 * @param {object} server - MCP server instance
 */
export function registerDatabaseResources(server) {
    logger.info('Registering database resources');
    
    // Ensure _resources exists
    if (!server._resources) {
        server._resources = {};
    }

    // Wrap the original resource method to add logging and error handling
    const originalResource = server.resource.bind(server);
    server.resource = function(name, uriPattern, handler) {
        const wrappedHandler = async function(...args) {
            logger.info(`Reading resource: ${name}`);
            logger.debug(`URI: ${args[0]?.href}`);
            
            try {
                const result = await handler(...args);
                logger.info(`Resource ${name} read successfully`);
                return result;
            } catch (err) {
                logger.error(`Resource ${name} read failed: ${err.message}`);
                
                // Format error for response
                const errorMessage = formatSqlError(err);
                
                return {
                    contents: [{
                        uri: args[0]?.href || `${name}://error`,
                        text: `Error reading resource: ${errorMessage}`
                    }]
                };
            }
        };
        
        // Store the resource for direct access
        server._resources[uriPattern] = {
            name: name,
            uriPattern: uriPattern,
            handler: wrappedHandler
        };
        
        logger.info(`Registered resource: ${name} at ${uriPattern}`);
        
        return originalResource(name, uriPattern, wrappedHandler);
    };
    
    // Register all database resources
    registerDatabaseSchemaResource(server);
    registerTablesListResource(server);
    registerProceduresListResource(server);
    registerFunctionsListResource(server);
    registerViewsListResource(server);
    registerIndexesListResource(server);
    
    logger.info('Database resources registered successfully');
}

/**
 * Register the database schema resource
 * @param {object} server - MCP server instance
 */
function registerDatabaseSchemaResource(server) {
    server.resource(
        "schema",
        "schema://database",
        async (uri) => {
            try {
                logger.info('Fetching database schema...');
                
                const result = await executeQuery(`
                    SELECT 
                        c.TABLE_NAME,
                        c.COLUMN_NAME,
                        c.DATA_TYPE,
                        c.IS_NULLABLE,
                        c.CHARACTER_MAXIMUM_LENGTH,
                        c.COLUMN_DEFAULT
                    FROM 
                        INFORMATION_SCHEMA.COLUMNS c
                    INNER JOIN
                        INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                    WHERE
                        t.TABLE_TYPE = 'BASE TABLE'
                    ORDER BY 
                        c.TABLE_NAME, c.ORDINAL_POSITION
                `);
                
                // Format schema data into human-readable text
                const formattedSchema = formatSchemaData(result.recordset);
                logger.info('Schema retrieved successfully');
                
                return {
                    contents: [{
                        uri: uri.href,
                        text: formattedSchema
                    }]
                };
            } catch (err) {
                logger.error(`Error retrieving schema: ${err.message}`);
                throw err;
            }
        }
    );
}

/**
 * Register the tables list resource
 * @param {object} server - MCP server instance
 */
function registerTablesListResource(server) {
    server.resource(
        "tables",
        "tables://list",
        async (uri) => {
            try {
                logger.info('Fetching tables list...');
                
                const result = await executeQuery(`
                    SELECT 
                        TABLE_SCHEMA,
                        TABLE_NAME,
                        TABLE_TYPE
                    FROM 
                        INFORMATION_SCHEMA.TABLES
                    WHERE 
                        TABLE_TYPE = 'BASE TABLE'
                    ORDER BY 
                        TABLE_SCHEMA, TABLE_NAME
                `);
                
                // Format as markdown list grouped by schema
                let markdown = `# Database Tables\n\n`;
                
                // Group by schema
                const tablesBySchema = {};
                result.recordset.forEach(table => {
                    if (!tablesBySchema[table.TABLE_SCHEMA]) {
                        tablesBySchema[table.TABLE_SCHEMA] = [];
                    }
                    tablesBySchema[table.TABLE_SCHEMA].push(table.TABLE_NAME);
                });
                
                // Add tables by schema
                for (const [schema, tables] of Object.entries(tablesBySchema)) {
                    markdown += `## ${schema} Schema\n\n`;
                    tables.forEach(table => {
                        markdown += `- ${table}\n`;
                    });
                    markdown += '\n';
                }
                
                logger.info(`Retrieved ${result.recordset.length} tables`);
                
                return {
                    contents: [{
                        uri: uri.href,
                        text: markdown
                    }]
                };
            } catch (err) {
                logger.error(`Error retrieving tables: ${err.message}`);
                throw err;
            }
        }
    );
}

/**
 * Register the stored procedures list resource
 * @param {object} server - MCP server instance
 */
function registerProceduresListResource(server) {
    server.resource(
        "procedures",
        "procedures://list",
        async (uri) => {
            try {
                logger.info('Fetching stored procedures list...');
                
                const result = await executeQuery(`
                    SELECT 
                        ROUTINE_SCHEMA,
                        ROUTINE_NAME
                    FROM 
                        INFORMATION_SCHEMA.ROUTINES
                    WHERE 
                        ROUTINE_TYPE = 'PROCEDURE'
                    ORDER BY 
                        ROUTINE_SCHEMA, ROUTINE_NAME
                `);
                
                // Format as markdown list grouped by schema
                let markdown = `# Database Stored Procedures\n\n`;
                
                // Group by schema
                const procsBySchema = {};
                result.recordset.forEach(proc => {
                    if (!procsBySchema[proc.ROUTINE_SCHEMA]) {
                        procsBySchema[proc.ROUTINE_SCHEMA] = [];
                    }
                    procsBySchema[proc.ROUTINE_SCHEMA].push(proc.ROUTINE_NAME);
                });
                
                // Add procedures by schema
                for (const [schema, procs] of Object.entries(procsBySchema)) {
                    markdown += `## ${schema} Schema\n\n`;
                    procs.forEach(proc => {
                        markdown += `- ${proc}\n`;
                    });
                    markdown += '\n';
                }
                
                logger.info(`Retrieved ${result.recordset.length} stored procedures`);
                
                return {
                    contents: [{
                        uri: uri.href,
                        text: markdown
                    }]
                };
            } catch (err) {
                logger.error(`Error retrieving stored procedures: ${err.message}`);
                throw err;
            }
        }
    );
}

/**
 * Register the functions list resource
 * @param {object} server - MCP server instance
 */
function registerFunctionsListResource(server) {
    server.resource(
        "functions",
        "functions://list",
        async (uri) => {
            try {
                logger.info('Fetching functions list...');
                
                const result = await executeQuery(`
                    SELECT 
                        ROUTINE_SCHEMA,
                        ROUTINE_NAME,
                        DATA_TYPE AS RETURN_TYPE
                    FROM 
                        INFORMATION_SCHEMA.ROUTINES
                    WHERE 
                        ROUTINE_TYPE = 'FUNCTION'
                    ORDER BY 
                        ROUTINE_SCHEMA, ROUTINE_NAME
                `);
                
                // Format as markdown list grouped by schema
                let markdown = `# Database Functions\n\n`;
                
                // Group by schema
                const funcsBySchema = {};
                result.recordset.forEach(func => {
                    if (!funcsBySchema[func.ROUTINE_SCHEMA]) {
                        funcsBySchema[func.ROUTINE_SCHEMA] = [];
                    }
                    funcsBySchema[func.ROUTINE_SCHEMA].push({
                        name: func.ROUTINE_NAME,
                        returnType: func.RETURN_TYPE
                    });
                });
                
                // Add functions by schema
                for (const [schema, funcs] of Object.entries(funcsBySchema)) {
                    markdown += `## ${schema} Schema\n\n`;
                    markdown += '| Function | Return Type |\n';
                    markdown += '|----------|------------|\n';
                    
                    funcs.forEach(func => {
                        markdown += `| ${func.name} | ${func.returnType} |\n`;
                    });
                    
                    markdown += '\n';
                }
                
                logger.info(`Retrieved ${result.recordset.length} functions`);
                
                return {
                    contents: [{
                        uri: uri.href,
                        text: markdown
                    }]
                };
            } catch (err) {
                logger.error(`Error retrieving functions: ${err.message}`);
                throw err;
            }
        }
    );
}

/**
 * Register the views list resource
 * @param {object} server - MCP server instance
 */
function registerViewsListResource(server) {
    server.resource(
        "views",
        "views://list",
        async (uri) => {
            try {
                logger.info('Fetching views list...');
                
                const result = await executeQuery(`
                    SELECT 
                        TABLE_SCHEMA,
                        TABLE_NAME
                    FROM 
                        INFORMATION_SCHEMA.VIEWS
                    ORDER BY 
                        TABLE_SCHEMA, TABLE_NAME
                `);
                
                // Format as markdown list grouped by schema
                let markdown = `# Database Views\n\n`;
                
                // Group by schema
                const viewsBySchema = {};
                result.recordset.forEach(view => {
                    if (!viewsBySchema[view.TABLE_SCHEMA]) {
                        viewsBySchema[view.TABLE_SCHEMA] = [];
                    }
                    viewsBySchema[view.TABLE_SCHEMA].push(view.TABLE_NAME);
                });
                
                // Add views by schema
                for (const [schema, views] of Object.entries(viewsBySchema)) {
                    markdown += `## ${schema} Schema\n\n`;
                    views.forEach(view => {
                        markdown += `- ${view}\n`;
                    });
                    markdown += '\n';
                }
                
                logger.info(`Retrieved ${result.recordset.length} views`);
                
                return {
                    contents: [{
                        uri: uri.href,
                        text: markdown
                    }]
                };
            } catch (err) {
                logger.error(`Error retrieving views: ${err.message}`);
                throw err;
            }
        }
    );
}

/**
 * Register the indexes list resource
 * @param {object} server - MCP server instance
 */
function registerIndexesListResource(server) {
    server.resource(
        "indexes",
        "indexes://list",
        async (uri) => {
            try {
                logger.info('Fetching indexes list...');
                
                const result = await executeQuery(`
                    SELECT 
                        s.name AS SchemaName,
                        t.name AS TableName,
                        i.name AS IndexName,
                        i.type_desc AS IndexType,
                        i.is_unique AS IsUnique,
                        i.is_primary_key AS IsPrimaryKey
                    FROM 
                        sys.indexes i
                    INNER JOIN 
                        sys.tables t ON i.object_id = t.object_id
                    INNER JOIN 
                        sys.schemas s ON t.schema_id = s.schema_id
                    WHERE 
                        i.name IS NOT NULL
                    ORDER BY 
                        s.name, t.name, i.name
                `);
                
                // Format as markdown table
                let markdown = `# Database Indexes\n\n`;
                
                // Group by table
                const indexesByTable = {};
                result.recordset.forEach(idx => {
                    const tableKey = `${idx.SchemaName}.${idx.TableName}`;
                    if (!indexesByTable[tableKey]) {
                        indexesByTable[tableKey] = [];
                    }
                    indexesByTable[tableKey].push({
                        name: idx.IndexName,
                        type: idx.IndexType,
                        isUnique: idx.IsUnique,
                        isPrimaryKey: idx.IsPrimaryKey
                    });
                });
                
                // Add indexes by table
                for (const [table, indexes] of Object.entries(indexesByTable)) {
                    markdown += `## ${table}\n\n`;
                    markdown += '| Index Name | Type | Unique | Primary Key |\n';
                    markdown += '|------------|------|--------|------------|\n';
                    
                    indexes.forEach(idx => {
                        markdown += `| ${idx.name} | ${idx.type} | ${idx.isUnique ? 'Yes' : 'No'} | ${idx.isPrimaryKey ? 'Yes' : 'No'} |\n`;
                    });
                    
                    markdown += '\n';
                }
                
                logger.info(`Retrieved ${result.recordset.length} indexes`);
                
                return {
                    contents: [{
                        uri: uri.href,
                        text: markdown
                    }]
                };
            } catch (err) {
                logger.error(`Error retrieving indexes: ${err.message}`);
                throw err;
            }
        }
    );
}

/**
 * Format schema data into human-readable text
 * @param {Array} records - Records from INFORMATION_SCHEMA.COLUMNS
 * @returns {string} - Formatted markdown
 */
function formatSchemaData(records) {
    const tables = {};
    
    // Group columns by table
    records.forEach(record => {
        if (!tables[record.TABLE_NAME]) {
            tables[record.TABLE_NAME] = [];
        }
        
        tables[record.TABLE_NAME].push({
            name: record.COLUMN_NAME,
            type: record.DATA_TYPE,
            length: record.CHARACTER_MAXIMUM_LENGTH,
            nullable: record.IS_NULLABLE === 'YES',
            default: record.COLUMN_DEFAULT
        });
    });
    
    // Format as text
    let output = '# Database Schema\n\n';
    
    for (const [tableName, columns] of Object.entries(tables)) {
        output += `## Table: ${tableName}\n\n`;
        output += '| Column | Type | Length | Nullable | Default |\n';
        output += '|--------|------|--------|----------|--------|\n';
        
        columns.forEach(col => {
            const length = col.length !== null ? col.length : 'N/A';
            const defaultVal = col.default !== null ? col.default : 'N/A';
            
            output += `| ${col.name} | ${col.type} | ${length} | ${col.nullable ? 'Yes' : 'No'} | ${defaultVal} |\n`;
        });
        
        output += '\n';
    }
    
    return output;
}