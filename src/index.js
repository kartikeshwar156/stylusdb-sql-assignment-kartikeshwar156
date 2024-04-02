// src/index.js

const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClause } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);

    // Parse the whereClause into column and value
    let [whereColumn, whereValue] = whereClause ? whereClause.split('=') : [null, null];
    whereColumn = whereColumn ? whereColumn.trim() : null;
    whereValue = whereValue ? whereValue.trim().replace(/'/g, '') : null;  // Remove quotes from value

    

    // Filter the data based on the whereClause
    const filteredData = whereColumn ? data.filter(row => row[whereColumn] === whereValue) : data;

    
    // Filter the fields based on the query
    return filteredData.map(row => {
        const filteredRow = {};
        fields.forEach(field => {
            filteredRow[field] = row[field];
        });
        return filteredRow;
    });
}

module.exports = executeSELECTQuery;