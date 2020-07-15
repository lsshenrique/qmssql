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
    constructor(sqlConfig) {
        this._sqlConfig = sqlConfig;
    }

    get pool() {
        return this._poolConnection;
    }

    initPool() {
        this._poolConnection = new mssql.ConnectionPool(this._sqlConfig).connect();
        return this._poolConnection;
    }

    async execute(sql) {
        const pool = await this._poolConnection; // ensures that the pool has been created

        try {
            let result = null

            if (typeof sql === 'string') {
                result = await pool.request().query(sql);
            } else {
                let request = pool.request()

                if (sql.params) {
                    request.inputs(sql.params)
                }

                if (typeof sql.query === 'function') {
                    result = await request.query(sql.query(request, mssql));
                } else {
                    result = await request.query(sql.query);
                }
            }

            return result.recordset || [];
        } catch (err) {
            console.log('SQL error', err, sql.query || sql);
            throw err
        }
    }

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
}

module.exports = MSSQL;
module.exports.native = mssql;