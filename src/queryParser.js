
// src/queryParser.js

function parseQuery(query) {
	// First, let's trim the query to remove any leading/trailing whitespaces

	console.log(12345)
	query = query.trim();

	const groupByRegex = /\sGROUP BY\s(.+)/i;
   const groupByMatch = query.match(groupByRegex);

   let groupByFields = null;
   if (groupByMatch) {
      groupByFields = groupByMatch[1].split(',').map(field => field.trim());
   }

	const groupsplit = query.split(/\sGROUP BY\s(.+)/i);
	query=groupsplit[0];

	let hasAggregateWithoutGroupBy= false;
	let hasAggregate = checkAggregateFunctions(query);

	if(hasAggregate && !groupByMatch)
	{
		hasAggregateWithoutGroupBy=true;
	}

	// Initialize variables for different parts of the query
	let selectPart, fromPart;

	// Split the query at the WHERE clause if it exists
	const whereSplit = query.split(/\sWHERE\s/i);
	query = whereSplit[0]; // Everything before WHERE clause

	// WHERE clause is the second part after splitting, if it exists
	const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

	

	// Split the remaining query at the JOIN clause if it exists
	const joinSplit = query.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
	selectPart = joinSplit[0].trim(); // Everything before JOIN clause

	// JOIN clause is the second part after splitting, if it exists
	const joinPart = joinSplit.length > 1 ? joinSplit[1].trim() : null;

	// Parse the SELECT part
	const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
	const selectMatch = selectPart.match(selectRegex);
	if (!selectMatch) {
		 throw new Error('Invalid SELECT format');
	}

	const [, fields, table] = selectMatch;

	// Parse the JOIN part if it exists
	let joinTable = null, joinCondition = null, joinType = null;
	if (joinPart) {
		//  const joinRegex = /^(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
		//  const joinMatch = joinPart.match(joinRegex);
		const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
      const joinMatch = query.match(joinRegex);

		if (!joinMatch) {
			throw new Error('Invalid JOIN format');
		}

		if (joinMatch) {
			
			joinType= joinMatch[1].trim(),
			joinTable= joinMatch[2].trim(),
			joinCondition= {
				left: joinMatch[3].trim(),
				right: joinMatch[4].trim()	
			};
	   }

		//  joinTable = joinMatch[1].trim();
		//  joinCondition = {
		// 	  left: joinMatch[2].trim(),
		// 	  right: joinMatch[3].trim()
		//  };
	}

	// Parse the WHERE part if it exists
	let whereClauses = [];
	if (whereClause) {
		 whereClauses = parseWhereClause(whereClause);
	}

	return {
		 fields: fields.split(',').map(field => field.trim()),
		 table: table.trim(),
		 whereClauses,
		 joinTable,
		 joinCondition,
		 joinType,
		 groupByFields,
		 hasAggregateWithoutGroupBy
	};
}

// src/queryParser.js
function parseWhereClause(whereString) {
	const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
	return whereString.split(/ AND | OR /i).map(conditionString => {
		 const match = conditionString.match(conditionRegex);
		 if (match) {
			  let [, field, operator, value] = match;
			// for string search in where
			//   value = parseIfJSON(value);

			return { field: field.trim(), operator, value: value.trim() };

		 }
		 throw new Error('Invalid WHERE clause format');
	});
}

function checkAggregateFunctions(query) {
	const aggregateFunctions = ["AVG", "COUNT", "SUM", "MIN", "MAX"];
	const upperCaseQuery = query.toUpperCase();

	for (let i = 0; i < aggregateFunctions.length; i++) {
		 if (upperCaseQuery.includes(aggregateFunctions[i])) {
			  return true;
		 }
	}

	return false;
}

function parseIfJSON(value) {
	try {
		 // Try to parse the value
		 let parsedValue = JSON.parse(value);
		 return parsedValue;
	} catch (e) {
		 // If it's not a valid JSON string, return the original value
		 return value;
	}
}

function parseJoinClause(query) {
	const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
	const joinMatch = query.match(joinRegex);

	if (joinMatch) {
		 return {
			  joinType: joinMatch[1].trim(),
			  joinTable: joinMatch[2].trim(),
			  joinCondition: {
					left: joinMatch[3].trim(),
					right: joinMatch[4].trim()
			  }
		 };
	}

	return {
		 joinType: null,
		 joinTable: null,
		 joinCondition: null
	};
}


module.exports = {parseQuery, parseJoinClause};