// src/index.js

const parseQuery = require("./queryParser");
const readCSV = require("./csvReader");

async function executeSELECTQuery(query) {
  // Now we will have joinTable, joinCondition in the parsed query
  const { fields, table, whereClauses, joinTable, joinCondition, joinType } =
    parseQuery(query);

  let data = await readCSV(`${table}.csv`);

  // Perform INNER JOIN if specified
  if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);

    switch (joinType.toUpperCase()) {
      case "INNER":
        data = performInnerJoin(data, joinData, joinCondition, fields, table);
        break;
      case "LEFT":
        data = performLeftJoin(data, joinData, joinCondition, fields, table);
        break;
      case "RIGHT":
        data = performRightJoin(data, joinData, joinCondition, fields, table);
        break;
      // Handle default case or unsupported JOIN types
    }
  }

  // Apply WHERE clause filtering
  const filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;

  // Select the specified fields
  return filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });
}

function evaluateCondition(row, clause) {
  const { field, operator, value } = clause;
  switch (operator) {
    case "=":
      return row[field] === value;
    case "!=":
      return row[field] !== value;
    case ">":
      return row[field] > value;
    case "<":
      return row[field] < value;
    case ">=":
      return row[field] >= value;
    case "<=":
      return row[field] <= value;
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

function performInnerJoin(data, joinData, joinCondition, fields, table) {
  data = data.flatMap((mainRow) => {
    return joinData
      .filter((joinRow) => {
        const mainValue = mainRow[joinCondition.left.split(".")[1]];
        const joinValue = joinRow[joinCondition.right.split(".")[1]];
        return mainValue === joinValue;
      })
      .map((joinRow) => {
        return fields.reduce((acc, field) => {
          const [tableName, fieldName] = field.split(".");
          acc[field] =
            tableName === table ? mainRow[fieldName] : joinRow[fieldName];
          return acc;
        }, {});
      });
  });

  return data;
}

function performLeftJoin(data, joinData, joinCondition, fields, table) {
  data = data.flatMap((mainRow) => {
    const joinRows = joinData.filter((joinRow) => {
      const mainValue = mainRow[joinCondition.left.split(".")[1]];
      const joinValue = joinRow[joinCondition.right.split(".")[1]];
      return mainValue === joinValue;
    });

    if (joinRows.length === 0) {
      joinRows.push({});
    }

    return joinRows.map((joinRow) => {
      return fields.reduce((acc, field) => {
        const [tableName, fieldName] = field.split(".");
        let value_given =
          tableName === table ? mainRow[fieldName] : joinRow[fieldName];

        if (value_given === undefined) 
        {
            value_given = null;
        }

        acc[field] = value_given;
        return acc;
      }, {});
    });
  });

  return data;
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
  [data, joinData] = [joinData, data];
  [joinCondition.left, joinCondition.right] = [
    joinCondition.right,
    joinCondition.left,
  ];

  data = data.flatMap((mainRow) => {
    const joinRows = joinData.filter((joinRow) => {
      const mainValue = mainRow[joinCondition.left.split(".")[1]];
      const joinValue = joinRow[joinCondition.right.split(".")[1]];
      return mainValue === joinValue;
    });

    if (joinRows.length === 0) {
      joinRows.push({});
    }

    return joinRows.map((joinRow) => {
      return fields.reduce((acc, field) => {
        const [tableName, fieldName] = field.split(".");
        let value_given =
          tableName === table ? mainRow[fieldName] : joinRow[fieldName];

        if (value_given === undefined) 
        {
            value_given = null;
        }
        
        acc[field] = value_given;
        return acc;
      }, {});
    });
  });

  return data;
}

module.exports = executeSELECTQuery;
