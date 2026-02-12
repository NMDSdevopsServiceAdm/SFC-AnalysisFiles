const trainingCoursesCreatedCount = () => {
  return (
    'COALESCE((' +
    ' SELECT COUNT(tc."UID") ' +
    ' FROM cqc."TrainingCourse" tc ' +
    ' WHERE tc."EstablishmentFK" = e."EstablishmentID" ' +
    '), 0) AS "trainingCoursesCreatedCount",'
  );
};



module.exports = {
trainingCoursesCreatedCount
}
