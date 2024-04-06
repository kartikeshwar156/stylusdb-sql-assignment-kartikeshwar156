// src/index.js

const {parseQuery, parseJoinClause} = require("./queryParser");
const readCSV = require("./csvReader");

function applyGroupBy(data, groupByFields, aggregateFunctions) {
  const groupedData = {};
let old_field = null;
let new_field = null;
let agg_func = null;

  data.forEach((row) => {
    const groupKey = groupByFields.map((field) => row[field]).join("_");
    
    if (!groupedData[groupKey]) {
      
      aggregateFunctions.forEach((func) => 
      {
        
        let [aggField, aggExpression] = func.split("(");

        if(aggExpression)
        {
          const [n_field,n_temp]= aggExpression.split(")");  
          old_field=n_field; 
          new_field=func;
          agg_func=aggField;
        }
      });
      
      groupedData[groupKey] = { ...row, count: 1 }; // Initialize with the first row's data
    
      
    } else {
      // Update aggregate values for existing group
      aggregateFunctions.forEach((func) => {
        let [aggField, aggExpression] = func.split("(");

        if(aggExpression)
        {
          const [n_field,n_temp]= aggExpression.split(")");  
          aggExpression=n_field;  
        }

        // if(aggField==="COUNT")
        // {
        //   groupedData[groupKey].count= 1
        // }

        console.log(aggField + " " + aggExpression);
        switch (aggField) {
          case "SUM":
            groupedData[groupKey][aggExpression] += row[aggExpression];
            break;
          case "COUNT":
            groupedData[groupKey].count += 1;
            break;
          case "AVG":
            groupedData[groupKey][aggExpression] += row[aggExpression];
            groupedData[groupKey].count += 1;
            break;
          case "COUNT":
            groupedData[groupKey].count += 1;
            break;
          case "MIN":
            groupedData[groupKey][aggExpression] = Math.min(groupedData[groupKey][aggExpression],row[aggExpression]);
            break;
          case "MAX":
            groupedData[groupKey][aggExpression] = Math.max(groupedData[groupKey][aggExpression],row[aggExpression]);
            break;
          
          // Add other aggregate functions (min, max, etc.) as needed
          default:
            break;
        }
      });
    }
    
  });
  
  console.log(old_field + " " + new_field + " " + agg_func + " ");
  
  

  // Convert grouped data back to an array of objects
  const result = Object.values(groupedData);
  
  result.map((row) => {
    switch(agg_func){
    case "COUNT":
      row["COUNT(*)"]=row.count;
      
      break;
    case "SUM":
      row[new_field]=row[old_field];
      
      break;
    case "AVG":
      row[new_field]=(row[old_field]/row.count);
      
      break;
    case "MAX":
      row[new_field]=row[old_field];
      
      break;
    case "MIN":
      row[new_field]=row[old_field];
      
      break;
    
    default:
      break;
      
      
    }
  });
  
  return(result);
}

function field_list_maker(data, table)
{
  const tableName = table;
  const fieldsToSelect = data[0] && Object.keys(data[0]).map(field => `${tableName}.${field}`);

  return fieldsToSelect;
}

async function executeSELECTQuery(query) {
  // Now we will have joinTable, joinCondition in the parsed query
  let {
    fields,
    table,
    whereClauses,
    joinTable,
    joinCondition,
    joinType,
    groupByFields,
    hasAggregateWithoutGroupBy
  } = parseQuery(query);

  // if (value.charAt(value.length - 1) === "'") {
  //   value = value.slice(2, -1);
  // }

  // for(let i = 0; i < whereClauses.length; i++) {
  //   if(whereClauses[i].value.charAt(whereClauses[i].value.length - 1) === "'") {
  //     whereClauses[i].value = whereClauses[i].value.slice(2, -1);
  //   }
  // }

  if(whereClauses.length > 0)
  {
    whereClauses[0]["value"] = whereClauses[0]["value"].replace(/"/g, '');

    if (whereClauses[0]["value"].charAt(whereClauses[0]["value"].length - 1) === "'") 
    {
      whereClauses[0]["value"]= whereClauses[0]["value"].slice(1, -1);
      console.log(whereClauses[0]["value"]);
      
    }
  }

  let data = await readCSV(`${table}.csv`);

  

  let list1=[];
  let list2=[];
  list1 = field_list_maker(data, table);

  if(joinTable)
  {
    let sec_data = await readCSV(`${joinTable}.csv`);
    list2 = field_list_maker(sec_data, joinTable);
    list1.push(...list2);
  }
 
  let field_list=list1;

  // Perform INNER JOIN if specified
  if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);

    switch (joinType.toUpperCase()) {
      case "INNER":
        data = performInnerJoin(data, joinData, joinCondition, field_list, table);
        break;
      case "LEFT":
        data = performLeftJoin(data, joinData, joinCondition, field_list, table);
        break;
      case "RIGHT":
        data = performRightJoin(data, joinData, joinCondition, field_list, table);
        break;
      // Handle default case or unsupported JOIN types
    }
  }

  // Apply WHERE clause filtering
  let filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;

  if (groupByFields) {
    filteredData = applyGroupBy(filteredData, groupByFields, fields);
  }

  // return filteredData;

  if(hasAggregateWithoutGroupBy)
  {
    filteredData= performAggregate(filteredData, fields);
  }

  // Select the specified fields
  return filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });
}

function performAggregate(filteredData, fields)
{

  let [aggField, aggExpression] = fields[0].split("(");
       
  const [n_field,n_temp]= aggExpression.split(")");  
  let old_field=n_field; 

  const groupedData = {};
  switch (aggField) {
    case "SUM":
      groupedData[fields[0]] = 0;
      break;
    case "COUNT":
      groupedData.count = 0;
      break;
    case "AVG":
      groupedData[fields[0]] = 0;
      groupedData.count = 0;
      break;
    case "MIN":
      groupedData[fields[0]] = 1000000;
      break;
    case "MAX":
      groupedData[fields[0]] = -1000000;
      break;
    
    // Add other aggregate functions (min, max, etc.) as needed
    default:
      break;
  }

  filteredData.forEach((row) => {
    
    switch (aggField) {
      case "SUM":
        groupedData[fields[0]] += Number(row[old_field]);
        break;
      case "COUNT":
        groupedData.count += 1;
        break;
      case "AVG":
        groupedData[fields[0]] += Number(row[old_field]);
        groupedData.count += 1;
        break;
      case "MIN":
        groupedData[fields[0]] = Math.min(groupedData[fields[0]],Number(row[old_field]));
        break;
      case "MAX":
        groupedData[fields[0]] = Math.max(groupedData[fields[0]],Number(row[old_field]));
        break;
      
      // Add other aggregate functions (min, max, etc.) as needed
      default:
        break;
    }
  });

  switch(aggField){
    case "COUNT":
      groupedData["COUNT(*)"]=groupedData.count;
      
      break;
    case "SUM":
      groupedData[fields[0]]= groupedData[fields[0]];
      
      break;
    case "AVG":
      groupedData[fields[0]]= groupedData[fields[0]]/groupedData.count;
      
      break;
    case "MAX":
      groupedData[fields[0]]= groupedData[fields[0]];
      
      break;
    case "MIN":
      groupedData[fields[0]]= groupedData[fields[0]];
      
      break;
    
    default:
      break;
            
    }

    let list_data=[];
    list_data.push(groupedData)

  return(list_data);
  
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

        if (value_given === undefined) {
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
          tableName === table ? joinRow[fieldName] : mainRow[fieldName];

        if (value_given === undefined) {
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
