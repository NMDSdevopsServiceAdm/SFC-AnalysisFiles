const { describe, it } = require('mocha');
const expect = require('chai').expect;

const {
  buildFindTrainingsByBatchQuery,
} = require('./batch');

describe('buildFindTrainingsByBatchQuery', () => {
  let sql;

  before(() => {
    sql = buildFindTrainingsByBatchQuery();
  });

  it('should include training certificate upload logic', () => {
    expect(sql).to.contain('TrainingCertificates');
    expect(sql).to.contain('Train_cert_upload');
  });

  it('should include training linked logic', () => {
    expect(sql).to.contain('Training_linked');
    expect(sql).to.contain('TrainingCourseFK');
  });

  it('should include training category join', () => {
    expect(sql).to.contain('LEFT JOIN "TrainingCategories"');
  });

  it('should include training provider join', () => {
    expect(sql).to.contain('LEFT JOIN "TrainingProvider"');
  });

  it('should filter by batch number', () => {
    expect(sql).to.contain('b."BatchNo" = ?');
  });

  it('should include worker training join', () => {
    expect(sql).to.contain('JOIN "WorkerTraining" t');
  });

  it('should include worker join', () => {
    expect(sql).to.contain('JOIN "Worker" w');
  });

  it('should include establishment join', () => {
    expect(sql).to.contain('FROM "Establishment" e');
  });

  it('should include training category output column', () => {
    expect(sql).to.contain('AS Training_category');
  });

  it('should include accredited training output column', () => {
    expect(sql).to.contain('AS Accredited_training');
  });

  it('should include training delivery output column', () => {
    expect(sql).to.contain('AS Training_delivery');
  });

  it('should include training type output column', () => {
    expect(sql).to.contain('AS Training_type');
  });

  it('should include training provider name output column', () => {
    expect(sql).to.contain('AS Training_provider_name');
  });

  it('should exclude archived establishments', () => {
    expect(sql).to.contain('e."Archived" = false');
  });

  it('should exclude archived workers', () => {
    expect(sql).to.contain('w."Archived" = false');
  });

  it('should exclude establishments with status', () => {
    expect(sql).to.contain('e."Status" IS NULL');
  });

  it('should order by establishment and worker', () => {
    expect(sql).to.contain('b."EstablishmentID"');
    expect(sql).to.contain('b."WorkerID"');
  });
});