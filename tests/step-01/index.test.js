const readCSV = require('../../src/csvReader');
const {parseQuery, parseJoinClause} = require('../../src/queryParser');
const executeSELECTQuery = require('../../src/index');

test('Average age of students above a certain age', async () => {
  const query = 'SELECT AVG(age) FROM student WHERE age > 22';
  const result = await executeSELECTQuery(query);
  const expectedAverage = (25 + 30 + 24) / 3; // Average age of students older than 22
  expect(result).toEqual([{ 'AVG(age)': expectedAverage }]);
});
// const result = await executeSELECTQuery(query);
//   expect(result).toEqual([
//       { course: "Mathematics", 'COUNT(*)': 2 }
//   ]);

// test('Execute SQL Query with LEFT JOIN with a WHERE clause filtering the join table',async () => {
//   const query = 'SELECT age, COUNT(*) FROM student WHERE age > 22 GROUP BY age';
//   const result = await executeSELECTQuery(query);
//   expect(result).toEqual([
//         { age: '24', 'COUNT(*)': 1 },
//         { age: '25', 'COUNT(*)': 1 },
//         { age: '30', 'COUNT(*)': 1 }
//     ]);
  
// });

// test('Basic Jest Test', () => {
//   expect(1).toBe(1);
// });

// const parsed = parseQuery(query);
  
//   expect(parsed).toEqual({
//     fields: ["age", "COUNT(*)"],
//     table: "student",
//     whereClauses: [
//       {
//         field: "age",
//         operator: ">",
//         value: "22",
//       },
//     ],
//     groupByFields: ['age'],
//     joinCondition:null,
//     joinTable: null,
//     joinType: null,
//     hasAggregateWithoutGroupBy: false,
//   });
