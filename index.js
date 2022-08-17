const mssql = require('mssql');

mssql.Request.prototype.inputs = function(params) {
    for (const key in params) {
        const param = params[key]

        if (typeof param === 'object' && param.type) {
            this.input(key, param.type, param.value)
        } else {
            this.input(key, param)
        }
    }

    return this
}

mssql.map.register(String, mssql.VarChar)

class MSSQL {
    constructor(sqlConfig, driverConfig = {}) {
        this._sqlConfig = sqlConfig;
        this.maxRetryToConnect = (driverConfig && driverConfig.maxRetryToConnect) || 3
    }

    get pool() {
        return this._poolConnection;
    }

    get serverAndPort() {
        return `${this._poolConnection.config.server}:${this._poolConnection.config.port}`;
    }

    async initPool() {
        this._poolConnection = new mssql.ConnectionPool(this._sqlConfig)

        return await this.createConnection()
            .catch((err) => console.log("Database Connection Failed!", err));
    }

    /**
     * function to execute stored procedure
     * @param {Object} sql Object containing the 'query' property with the query and the 'params' property with the parameters to be sent to the query
     * @returns returns the result of executing the query
     */
    async execute(sql, options) {
        let conn = null;
        let retry = 0
        let connId = Date.now()

        do {
            try {
                conn = await this.getConnection();

                if (retry > 0) {
                    console.log(`#${connId} - SQL connection re-established`);
                }

                let result = null

                if (options && options.debug) {
                    console.log(debugQuery(sql))
                }

                if (typeof sql === 'string') {
                    result = await conn.request().query(sql);
                } else {
                    let request = conn.request()

                    if (sql.params) {
                        request.inputs(sql.params)
                    }

                    if (typeof sql.query === 'function') {
                        result = await request.query(sql.query(request, mssql));
                    } else {
                        result = await request.query(sql.query);
                    }
                }

                if (options && options.returnMultipleSets) {
                    return result.recordsets || [];
                }

                return result.recordset || [];
            } catch (err) {
                if (isErrorConnection(err) && retry < this.maxRetryToConnect) {
                    retry++;
                    console.error(`#${connId} - SQL error connection, executing retry ${retry}/${this.maxRetryToConnect}`);

                    await this.closeConnection()

                    if (!isFirstRetry(retry)) {
                        await sleep(2000)
                    }
                } else {
                    console.error('SQL error', err, sql.query || sql);
                    throw err
                }

            }
        } while (retry <= this.maxRetryToConnect);

    }

    /**
     * function to execute stored procedure
     * @param {Object} sql Object containing the 'query' property with the store procedure and the 'params' property with the parameters to be sent to the store procedure
     * @returns returns the result of executing the procedure 
     */
    async executeSP(sql) {
        const pool = await this._poolConnection; // ensures that the pool has been created

        try {
            let result = null

            if (typeof sql === 'string') {
                result = await pool.request().execute(sql);
            } else {
                let request = pool.request()

                if (sql.params) {
                    request.inputs(sql.params)
                }

                if (typeof sql.query === 'function') {
                    result = await request.execute(sql.execute(request, mssql));
                } else {
                    result = await request.execute(sql.query);
                }
            }

            return result.recordset || [];
        } catch (err) {
            console.log('SQL error', err, sql.query || sql);
            throw err
        }
    }

    async getConnection() {
        if (this._taskConnecting) {
            await this._taskConnecting
        }

        if (!this._poolConnection.connected) {
            await this.createConnection()
        }

        return this._poolConnection
    }

    async createConnection() {
        if (this._taskConnecting) {
            return await this._taskConnecting
        }

        try {
            this._taskConnecting = this._poolConnection
                .connect()
                .then(() => this.logConnected())

            return await this._taskConnecting

        } catch (error) {
            throw error
        } finally {
            this._taskConnecting = null
        }

    }

    async closeConnection() {
        await this.pool.close()
    }

    logConnected() {
        const now = new Date()
        console.log(`Connected to MSSQL: ${this.serverAndPort} on ${now.toLocaleDateString("pt-br")} ${now.toLocaleTimeString("pt-br")}`)
    }
}

function isFirstRetry(cont) {
    return cont > 1
}

function isErrorConnection(err) {
    return (err.code == "ESOCKET" && err.message.indexOf("Failed to connect") >= 0) ||
        (err.code == "EREQUEST" && err.message.indexOf("Connection lost") >= 0)
}

function sleep(interval) {
    return new Promise((resolve) => setTimeout(() => resolve(), interval))
}

function debugQuery(sql) {
    let declare = []
    let params = (sql && sql.params) || []
    let query = sql.query || sql

    for (const key in params) {
        let param = params[key]

        if (param && param.value != null) {
            let type = param.type.name
            let value = param.value

            if (param.type.type) {
                type = param.type.type.name

                if (param.type.length) {
                    type = `${type}(${param.type.length})`
                }
            }

            if (typeof value === 'boolean') {
                value = value ? 1 : 0
            }

            declare.push(`@${key} AS ${type} = ${JSON.stringify(value)}`)
        }
    }

    let str = ''

    if (declare.length) {
        str = "DECLARE \n\t" + declare.join(',\n\t').replace(/\"/g, '\'')
    }

    return str + "\n" + query
}

module.exports = MSSQL;
module.exports.native = mssql;