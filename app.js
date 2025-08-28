// Simple in-memory SQL engine for basic operations
class SimpleSQL {
    constructor() {
        this.tables = new Map();
    }

    exec(query) {
        const trimmed = query.trim().toLowerCase();
        
        if (trimmed.startsWith('create table')) {
            return this.createTable(query);
        } else if (trimmed.startsWith('insert into')) {
            return this.insertInto(query);
        } else if (trimmed.startsWith('select')) {
            return this.select(query);
        } else if (trimmed.startsWith('drop table')) {
            return this.dropTable(query);
        } else if (trimmed.startsWith('read_csv_auto(') || trimmed.startsWith('read_parquet(')) {
            // Handle DuckDB file reading functions
            throw new Error(`File reading functions like read_csv_auto() and read_parquet() are not supported in the local SQL engine.

For advanced DuckDB features like:
• read_csv_auto('https://example.com/file.csv')  
• read_parquet('https://example.com/file.parquet')

Please use the full DuckDB WASM version instead of this local engine.`);
        } else {
            throw new Error(`Unsupported query type. This simple SQL engine supports: CREATE TABLE, INSERT INTO, SELECT, DROP TABLE

For advanced DuckDB features, please use the full DuckDB WASM version.`);
        }
    }

    run(query) {
        this.exec(query);
    }

    createTable(query) {
        const match = query.match(/create\s+table\s+(\w+)\s*\((.*)\)/i);
        if (!match) throw new Error('Invalid CREATE TABLE syntax');
        
        const tableName = match[1].toLowerCase();
        const columnsStr = match[2];
        
        const columns = columnsStr.split(',').map(col => {
            const parts = col.trim().split(/\s+/);
            return { name: parts[0], type: parts[1] || 'TEXT' };
        });
        
        this.tables.set(tableName, { columns, rows: [] });
        return [];
    }

    insertInto(query) {
        const match = query.match(/insert\s+into\s+(\w+)\s+values\s*\((.*)\)/i);
        if (!match) throw new Error('Invalid INSERT syntax');
        
        const tableName = match[1].toLowerCase();
        const valuesStr = match[2];
        
        if (!this.tables.has(tableName)) {
            throw new Error(`Table '${tableName}' does not exist`);
        }
        
        const table = this.tables.get(tableName);
        
        // Parse multiple value sets: (1, 'John'), (2, 'Jane')
        const valuesSets = this.parseValuesSets(valuesStr);
        
        valuesSets.forEach(values => {
            if (values.length !== table.columns.length) {
                throw new Error(`Column count mismatch. Expected ${table.columns.length}, got ${values.length}`);
            }
            table.rows.push(values);
        });
        
        return [];
    }

    parseValuesSets(valuesStr) {
        const sets = [];
        let current = '';
        let inQuotes = false;
        let parenCount = 0;
        
        for (let i = 0; i < valuesStr.length; i++) {
            const char = valuesStr[i];
            
            if (char === "'" && (i === 0 || valuesStr[i-1] !== '\\')) {
                inQuotes = !inQuotes;
            }
            
            if (!inQuotes) {
                if (char === '(') parenCount++;
                if (char === ')') parenCount--;
            }
            
            if (!inQuotes && parenCount === 0 && char === ',') {
                if (current.trim()) {
                    sets.push(this.parseValues(current.trim()));
                }
                current = '';
            } else {
                current += char;
            }
        }
        
        if (current.trim()) {
            sets.push(this.parseValues(current.trim()));
        }
        
        return sets;
    }

    parseValues(valuesStr) {
        // Remove outer parentheses
        valuesStr = valuesStr.replace(/^\(|\)$/g, '');
        
        const values = [];
        let current = '';
        let inQuotes = false;
        
        for (let i = 0; i < valuesStr.length; i++) {
            const char = valuesStr[i];
            
            if (char === "'" && (i === 0 || valuesStr[i-1] !== '\\')) {
                inQuotes = !inQuotes;
                continue;
            }
            
            if (!inQuotes && char === ',') {
                values.push(this.parseValue(current.trim()));
                current = '';
            } else {
                current += char;
            }
        }
        
        if (current.trim()) {
            values.push(this.parseValue(current.trim()));
        }
        
        return values;
    }

    parseValue(value) {
        value = value.trim();
        
        if (value.toLowerCase() === 'null') return null;
        if (value === 'true') return true;
        if (value === 'false') return false;
        
        // Try to parse as number
        if (/^-?\d+$/.test(value)) return parseInt(value, 10);
        if (/^-?\d*\.\d+$/.test(value)) return parseFloat(value);
        
        return value;
    }

    select(query) {
        // For flexible SELECT statements (including DuckDB functions and external files),
        // we'll try basic parsing first, but if it doesn't match our simple pattern,
        // we'll return a helpful message instead of throwing an error
        
        const basicMatch = query.match(/select\s+(.*?)\s+from\s+(\w+)(?:\s+where\s+(.*))?/i);
        
        if (basicMatch) {
            // Handle basic SELECT from local tables
            const columnsStr = basicMatch[1].trim();
            const tableName = basicMatch[2].toLowerCase();
            const whereClause = basicMatch[3];
            
            if (!this.tables.has(tableName)) {
                throw new Error(`Table '${tableName}' does not exist`);
            }
            
            const table = this.tables.get(tableName);
            let rows = [...table.rows];
            
            // Apply WHERE clause
            if (whereClause) {
                rows = rows.filter(row => this.evaluateWhere(row, table.columns, whereClause));
            }
            
            // Select columns
            let columns, values;
            if (columnsStr === '*') {
                columns = table.columns.map(col => col.name);
                values = rows;
            } else {
                const selectedCols = columnsStr.split(',').map(col => col.trim());
                columns = selectedCols;
                
                const colIndices = selectedCols.map(colName => {
                    const index = table.columns.findIndex(col => col.name.toLowerCase() === colName.toLowerCase());
                    if (index === -1) throw new Error(`Column '${colName}' not found`);
                    return index;
                });
                
                values = rows.map(row => colIndices.map(index => row[index]));
            }
            
            return [{ columns, values }];
        } else {
            // For advanced queries like SELECT from URLs or DuckDB functions
            throw new Error(`Advanced SELECT queries (like reading from URLs or using functions) are not supported in the local SQL engine. 
            
Supported format: SELECT columns FROM table_name [WHERE condition]
            
For advanced DuckDB features like:
• SELECT * FROM 'https://example.com/file.parquet'
• read_csv_auto('https://example.com/file.csv')

Please use the full DuckDB WASM version instead of this local engine.`);
        }
    }

    evaluateWhere(row, columns, whereClause) {
        // Simple WHERE evaluation - supports basic comparisons
        const match = whereClause.match(/(\w+)\s*([><=]+)\s*(.+)/i);
        if (!match) return true; // If we can't parse it, include the row
        
        const [, columnName, operator, valueStr] = match;
        const columnIndex = columns.findIndex(col => col.name.toLowerCase() === columnName.toLowerCase());
        
        if (columnIndex === -1) return true;
        
        const rowValue = row[columnIndex];
        const compareValue = this.parseValue(valueStr);
        
        switch (operator) {
            case '=': return rowValue == compareValue;
            case '>': return rowValue > compareValue;
            case '<': return rowValue < compareValue;
            case '>=': return rowValue >= compareValue;
            case '<=': return rowValue <= compareValue;
            case '!=':
            case '<>': return rowValue != compareValue;
            default: return true;
        }
    }

    dropTable(query) {
        const match = query.match(/drop\s+table\s+(\w+)/i);
        if (!match) throw new Error('Invalid DROP TABLE syntax');
        
        const tableName = match[1].toLowerCase();
        if (!this.tables.has(tableName)) {
            throw new Error(`Table '${tableName}' does not exist`);
        }
        
        this.tables.delete(tableName);
        return [];
    }
}

class DuckDBWorksheet {
    constructor() {
        this.db = null;
        this.connection = null;
        this.isInitialized = false;
        this.isFallback = false;
        this.fallbackDb = null;
        this.init();
        this.bindEvents();
    }

    async init() {
        console.log('DuckDBWorksheet.init() called');
        try {
            this.setStatus('Initializing DuckDB WASM...');
            
            // Try to initialize DuckDB WASM first
            const duckdb = window.duckdb || window.DuckDB;
            console.log('DuckDB object:', duckdb);
            if (!duckdb) {
                throw new Error('DuckDB WASM library not loaded');
            }
            
            // Use CDN bundles but create a local proxy worker to avoid CORS
            const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
            console.log('Available CDN bundles:', JSDELIVR_BUNDLES);
            const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
            console.log('Selected bundle:', bundle);
            
            // Create a proxy worker that imports the CDN worker
            const workerScript = `importScripts('${bundle.mainWorker}');`;
            const blob = new Blob([workerScript], { type: 'application/javascript' });
            const workerUrl = URL.createObjectURL(blob);
            const worker = new Worker(workerUrl);
            const logger = new duckdb.ConsoleLogger();
            this.db = new duckdb.AsyncDuckDB(logger, worker);
            
            console.log('Instantiating with:', { mainModule: bundle.mainModule, pthreadWorker: bundle.pthreadWorker });
            await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
            this.connection = await this.db.connect();
            
            this.isInitialized = true;
            this.isFallback = false;
            this.setStatus('Ready - DuckDB WASM initialized');
            document.getElementById('executeBtn').disabled = false;
            
        } catch (error) {
            console.error('Failed to initialize DuckDB WASM, falling back to SimpleSQL:', error);
            
            // Fallback to SimpleSQL
            try {
                this.setStatus('Falling back to SimpleSQL engine...');
                this.fallbackDb = new SimpleSQL();
                this.isInitialized = true;
                this.isFallback = true;
                this.setStatus('Ready - Using SimpleSQL fallback');
                document.getElementById('executeBtn').disabled = false;
            } catch (fallbackError) {
                console.error('Failed to initialize fallback engine:', fallbackError);
                this.showError('Failed to initialize any SQL engine: ' + fallbackError.message);
                this.setStatus('Initialization failed');
            }
        }
    }

    bindEvents() {
        console.log('Binding events...');
        const executeBtn = document.getElementById('executeBtn');
        const clearBtn = document.getElementById('clearBtn');
        const queryEditor = document.getElementById('queryEditor');

        console.log('Execute button:', executeBtn);
        console.log('Clear button:', clearBtn);
        
        executeBtn.addEventListener('click', () => {
            console.log('Execute button clicked');
            this.executeQuery();
        });
        clearBtn.addEventListener('click', () => this.clearQuery());
        
        queryEditor.addEventListener('keydown', (e) => {
            if (e.ctrlKey && e.key === 'Enter') {
                e.preventDefault();
                this.executeQuery();
            }
        });

        executeBtn.disabled = !this.isInitialized;
        console.log('Execute button disabled:', executeBtn.disabled);
        console.log('Is initialized:', this.isInitialized);
    }

    async executeQuery() {
        if (!this.isInitialized) {
            this.showError('Database is not initialized yet. Please wait...');
            return;
        }

        const query = document.getElementById('queryEditor').value.trim();
        if (!query) {
            this.showError('Please enter a SQL query');
            return;
        }

        const startTime = performance.now();
        this.showLoading(true);
        this.setStatus('Executing query...');

        try {
            if (this.isFallback) {
                // Handle SimpleSQL fallback
                const statements = this.parseStatements(query);
                let lastResult = null;
                
                for (const statement of statements) {
                    const trimmedStatement = statement.trim();
                    if (!trimmedStatement) continue;
                    
                    if (this.isSelectStatement(trimmedStatement)) {
                        const result = this.fallbackDb.exec(trimmedStatement);
                        if (result.length > 0) {
                            lastResult = {
                                columns: result[0].columns,
                                values: result[0].values,
                                numRows: result[0].values.length
                            };
                        }
                    } else {
                        this.fallbackDb.run(trimmedStatement);
                    }
                }
                
                const endTime = performance.now();
                const executionTime = Math.round(endTime - startTime);
                
                if (lastResult) {
                    this.displayFallbackResults(lastResult, executionTime);
                } else {
                    this.showSuccess(`Query executed successfully in ${executionTime}ms`);
                }
                
            } else {
                // Handle DuckDB WASM
                const statements = this.parseStatements(query);
                let lastResult = null;
                
                for (const statement of statements) {
                    const trimmedStatement = statement.trim();
                    if (!trimmedStatement) continue;
                    
                    if (this.isSelectStatement(trimmedStatement)) {
                        const result = await this.connection.query(trimmedStatement);
                        lastResult = result;
                    } else {
                        await this.connection.query(trimmedStatement);
                    }
                }
                
                const endTime = performance.now();
                const executionTime = Math.round(endTime - startTime);
                
                if (lastResult) {
                    this.displayResults(lastResult, executionTime);
                } else {
                    this.showSuccess(`Query executed successfully in ${executionTime}ms`);
                }
            }
            
            this.setStatus('Ready');
            
        } catch (error) {
            console.error('Query execution error:', error);
            this.showError('Query Error: ' + error.message);
            this.setStatus('Error');
        } finally {
            this.showLoading(false);
        }
    }

    parseStatements(query) {
        return query.split(';').filter(stmt => stmt.trim());
    }

    isSelectStatement(statement) {
        const trimmed = statement.trim().toLowerCase();
        return trimmed.startsWith('select') || 
               trimmed.startsWith('show') || 
               trimmed.startsWith('describe') ||
               trimmed.startsWith('explain') ||
               trimmed.startsWith('with');
    }

    displayResults(result, executionTime) {
        const resultsDiv = document.getElementById('results');
        
        if (result.numRows === 0) {
            resultsDiv.innerHTML = `
                <div class="success">Query executed successfully in ${executionTime}ms - No rows returned</div>
                <p style="color: #7f8c8d; text-align: center; padding: 2rem;">No data to display</p>
            `;
            return;
        }

        const columns = result.schema.fields.map(field => field.name);
        const rows = result.toArray().map(row => Object.values(row));
        
        this.currentResultData = { columns, rows };
        
        let html = `
            <div class="success">
                Query executed successfully in ${executionTime}ms - ${result.numRows} row(s) returned
                <button id="downloadCsvBtn" class="download-btn" onclick="window.worksheet.downloadCSV()">Download CSV</button>
            </div>
            <table class="results-table">
                <thead>
                    <tr>
                        ${columns.map(col => `<th>${this.escapeHtml(col)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
        `;
        
        rows.forEach(row => {
            html += '<tr>';
            row.forEach(cell => {
                const cellValue = cell === null ? '<em>NULL</em>' : this.escapeHtml(String(cell));
                html += `<td>${cellValue}</td>`;
            });
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        resultsDiv.innerHTML = html;
    }

    displayFallbackResults(result, executionTime) {
        const resultsDiv = document.getElementById('results');
        
        if (result.numRows === 0) {
            resultsDiv.innerHTML = `
                <div class="success">Query executed successfully in ${executionTime}ms - No rows returned</div>
                <p style="color: #7f8c8d; text-align: center; padding: 2rem;">No data to display</p>
            `;
            return;
        }

        const columns = result.columns;
        const rows = result.values;
        
        this.currentResultData = { columns, rows };
        
        let html = `
            <div class="success">
                Query executed successfully in ${executionTime}ms - ${result.numRows} row(s) returned
                <button id="downloadCsvBtn" class="download-btn" onclick="window.worksheet.downloadCSV()">Download CSV</button>
            </div>
            <table class="results-table">
                <thead>
                    <tr>
                        ${columns.map(col => `<th>${this.escapeHtml(col)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
        `;
        
        rows.forEach(row => {
            html += '<tr>';
            row.forEach(cell => {
                const cellValue = cell === null ? '<em>NULL</em>' : this.escapeHtml(String(cell));
                html += `<td>${cellValue}</td>`;
            });
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        resultsDiv.innerHTML = html;
    }

    showError(message) {
        const resultsDiv = document.getElementById('results');
        resultsDiv.innerHTML = `<div class="error">${this.escapeHtml(message)}</div>`;
    }

    showSuccess(message) {
        const resultsDiv = document.getElementById('results');
        resultsDiv.innerHTML = `<div class="success">${this.escapeHtml(message)}</div>`;
    }

    showLoading(show) {
        const loading = document.getElementById('loading');
        const executeBtn = document.getElementById('executeBtn');
        
        loading.style.display = show ? 'block' : 'none';
        executeBtn.disabled = show || !this.isInitialized;
        executeBtn.textContent = show ? 'Executing...' : 'Execute Query';
    }

    clearQuery() {
        document.getElementById('queryEditor').value = '';
        document.getElementById('results').innerHTML = `
            <p style="color: #7f8c8d; text-align: center; padding: 2rem;">
                Execute a query to see results here
            </p>
        `;
        this.setStatus('Ready');
    }

    setStatus(status, additional = '') {
        document.getElementById('statusText').textContent = status;
        document.getElementById('executionTime').textContent = additional;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    downloadCSV() {
        if (!this.currentResultData) {
            alert('No query results available to download');
            return;
        }

        const { columns, rows } = this.currentResultData;
        
        let csvContent = columns.map(col => this.escapeCsv(col)).join(',') + '\n';
        
        rows.forEach(row => {
            const csvRow = row.map(cell => {
                if (cell === null || cell === undefined) {
                    return '';
                }
                return this.escapeCsv(String(cell));
            }).join(',');
            csvContent += csvRow + '\n';
        });

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        
        link.setAttribute('href', url);
        link.setAttribute('download', 'query_results.csv');
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    escapeCsv(value) {
        if (typeof value !== 'string') {
            value = String(value);
        }
        
        if (value.includes(',') || value.includes('"') || value.includes('\n') || value.includes('\r')) {
            return '"' + value.replace(/"/g, '""') + '"';
        }
        return value;
    }

    async cleanup() {
        try {
            if (this.connection) {
                await this.connection.close();
                this.connection = null;
            }
            if (this.db) {
                await this.db.terminate();
                this.db = null;
            }
        } catch (error) {
            console.error('Error during cleanup:', error);
        }
    }
}

function initializeApp() {
    console.log('Initializing app...');
    const worksheet = new DuckDBWorksheet();
    window.worksheet = worksheet;
    console.log('DuckDBWorksheet created:', worksheet);
    
    window.addEventListener('beforeunload', () => {
        worksheet.cleanup();
    });
}

// Check if DOM is already loaded or wait for it
if (document.readyState === 'loading') {
    console.log('DOM still loading, waiting for DOMContentLoaded...');
    document.addEventListener('DOMContentLoaded', initializeApp);
} else {
    console.log('DOM already loaded, initializing immediately...');
    initializeApp();
}