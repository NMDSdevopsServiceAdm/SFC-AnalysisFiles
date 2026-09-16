
const { describe, it} = require('mocha');
const expect = require('chai').expect;
const {
  nursesQuestionAnswersColumn,
  generateSqlQueriesForNursesQuestionAnswersColumns,
} = require('./nurses-questions');

describe('nursesQuestionAnswersColumn', () => {
  it('should generate the SQL for a single nurse field of practice', () => {
    expect(
      nursesQuestionAnswersColumn(1, 'jr16fop1')
    ).equal(
      '(SELECT "AnalysisFileCode" FROM "NurseFieldOfPractice" n ' +
      'JOIN "WorkerNurseFieldsOfPractice" wn on n."ID" = wn."NurseFieldOfPracticeID" ' +
      'WHERE w."ID" = wn."WorkerID" AND "NurseFieldOfPracticeID"= 1 LIMIT 1) jr16fop1,'
    );
  });
});

describe('generateSqlQueriesForNursesQuestionAnswersColumns', () => {
  it('should generate SQL for multiple nurse field of practice mappings', () => {
    const mappings = [
      { id: 1, nursesQuestionAnswersCode: 'jr16fop1' },
      { id: 2, nursesQuestionAnswersCode: 'jr16fop2' },
    ];

    expect(
      generateSqlQueriesForNursesQuestionAnswersColumns(mappings)
    ).equal(
      '(SELECT "AnalysisFileCode" FROM "NurseFieldOfPractice" n ' +
        'JOIN "WorkerNurseFieldsOfPractice" wn on n."ID" = wn."NurseFieldOfPracticeID" ' +
        'WHERE w."ID" = wn."WorkerID" AND "NurseFieldOfPracticeID"= 1 LIMIT 1) jr16fop1,\n' +
        '(SELECT "AnalysisFileCode" FROM "NurseFieldOfPractice" n ' +
        'JOIN "WorkerNurseFieldsOfPractice" wn on n."ID" = wn."NurseFieldOfPracticeID" ' +
        'WHERE w."ID" = wn."WorkerID" AND "NurseFieldOfPracticeID"= 2 LIMIT 1) jr16fop2,'
    );
  });

  it('should return an empty string when no mappings are provided', () => {
    expect(
      generateSqlQueriesForNursesQuestionAnswersColumns([])
    ).equal('');
  });
});