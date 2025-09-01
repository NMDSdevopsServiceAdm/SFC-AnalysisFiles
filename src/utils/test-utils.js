const removeIndentation = (sqlStatement) => sqlStatement.replace(/\s+/g, ' ').trim();

module.exports = {
  removeIndentation,
};