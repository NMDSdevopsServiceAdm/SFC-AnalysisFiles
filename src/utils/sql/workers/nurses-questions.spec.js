
const { expect } = require('chai');

const {
  nursesQuestionAnswersColumn,
  generateSqlQueriesForNursesQuestionAnswersColumns,
} = require('./nurses-questions');

describe('nursesQuestionAnswersColumn', () => {
  it('should return 1 when the worker has answered the nurse field', () => {
    expect(
      nursesQuestionAnswersColumn(1, 'jr16fop1')
    ).to.equal(
      'CASE ' +
      'WHEN EXISTS (' +
        'SELECT 1 ' +
        'FROM "WorkerNurseFieldsOfPractice" wn ' +
        'WHERE wn."WorkerID" = w."ID" ' +
        'AND wn."NurseFieldOfPracticeID" = 1' +
      ') THEN 1 ' +
      'WHEN w."MainJobFKValue" = 23 ' +
        'AND NOT EXISTS (' +
          'SELECT 1 ' +
          'FROM "WorkerNurseFieldsOfPractice" wn ' +
          'WHERE wn."WorkerID" = w."ID"' +
        ') THEN -1 ' +
      'ELSE 0 ' +
      'END jr16fop1,'
    );
  });

  it('should generate the correct SQL for different nurse fields', () => {
    expect(
      nursesQuestionAnswersColumn(2, 'jr16fop2')
    ).to.include('AND wn."NurseFieldOfPracticeID" = 2');

    expect(
      nursesQuestionAnswersColumn(2, 'jr16fop2')
    ).to.include('END jr16fop2,');
  });
});

describe('generateSqlQueriesForNursesQuestionAnswersColumns', () => {
  it('should generate SQL for all nurse field mappings', () => {
    const mappings = [
      { id: 1, nursesQuestionAnswersCode: 'jr16fop1' },
      { id: 2, nursesQuestionAnswersCode: 'jr16fop2' },
      { id: 3, nursesQuestionAnswersCode: 'jr16fop3' },
      { id: 4, nursesQuestionAnswersCode: 'jr16fop4' },
    ];

    const result =
      generateSqlQueriesForNursesQuestionAnswersColumns(mappings);

    expect(result).to.include('END jr16fop1,');
    expect(result).to.include('END jr16fop2,');
    expect(result).to.include('END jr16fop3,');
    expect(result).to.include('END jr16fop4,');
  });

  it('should generate the correct field IDs', () => {
    const mappings = [
      { id: 1, nursesQuestionAnswersCode: 'jr16fop1' },
      { id: 2, nursesQuestionAnswersCode: 'jr16fop2' },
      { id: 3, nursesQuestionAnswersCode: 'jr16fop3' },
      { id: 4, nursesQuestionAnswersCode: 'jr16fop4' },
    ];

    const result =
      generateSqlQueriesForNursesQuestionAnswersColumns(mappings);

    expect(result).to.include('AND wn."NurseFieldOfPracticeID" = 1');
    expect(result).to.include('AND wn."NurseFieldOfPracticeID" = 2');
    expect(result).to.include('AND wn."NurseFieldOfPracticeID" = 3');
    expect(result).to.include('AND wn."NurseFieldOfPracticeID" = 4');
  });

  it('should return an empty string when no mappings are provided', () => {
    expect(
      generateSqlQueriesForNursesQuestionAnswersColumns([])
    ).to.equal('');
  });
});
