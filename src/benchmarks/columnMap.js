const reportToDbColumnMap = new Map([
  ['establishmentid', 'EstablishmentFK'],
  ['Main_Job_Role', 'MainJobRole'],
  ['Local_Authority_Area', 'LocalAuthorityArea'],
  ['Main_Service', 'MainServiceFK'],
  ['Base_Establishments', 'BaseEstablishments'],
  ['Base_Workers', 'BaseWorkers'],
  ['CQC_Rating_Overall_OutstGood', 'CQCGoodOutstandingRating'],
  ['HourlyRate_mean', 'AverageHourlyRate'],
  ['Annual_FTE_mean', 'AverageAnnualFTE'],
  ['Count_Has_SC_Qual', 'CountHasSCQual'],
  ['Count_No_SC_Qual', 'CountNoSCQual'],
  ['Qualifications', 'Qualifications'],
  ['Workers_ForSickness', 'WorkersForSickness'],
  ['Sickness_Mean', 'AverageNoOfSickDays'],
  ['Employees', 'Employees'],
  ['Leavers', 'Leavers'],
  ['Worker_Count', 'WorkerCount'],
  ['Turnover_Rate', 'TurnoverRate'],
  ['Vacancies', 'Vacancies'],
  ['Vacancy_Rate', 'VacancyRate'],
]);

module.exports = {
  reportToDbColumnMap,
};
