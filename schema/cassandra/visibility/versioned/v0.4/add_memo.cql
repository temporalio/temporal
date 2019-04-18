ALTER TABLE open_executions ADD memo blob;
ALTER TABLE closed_executions ADD memo blob;
ALTER TABLE closed_executions_v2 ADD memo blob;
ALTER TABLE open_executions ADD encoding text;
ALTER TABLE closed_executions ADD encoding text;
ALTER TABLE closed_executions_v2 ADD encoding text;
