INSERT INTO sunbird_courses.course_batch (courseid, batchid, createdby, createddate, enddate, end_date, enrollmentenddate, name, startdate, start_date, status) VALUES ('do_1130928636168192001667', 'batch-001', 'manju', '2020-10-10', '2020-01-10', '2020-01-10', '2020-01-01', 'Basic Java, test', '2020-01-20', '2020-01-20', 1 );
INSERT INTO sunbird_courses.course_batch (courseid, batchid, createdby, createddate, enddate, end_date, enrollmentenddate, name, startdate, start_date, status) VALUES ('do_1130928636168192001668', 'batch-002', 'manju', '2020-10-10', '2020-01-10', '2020-01-10', '2020-01-01', 'Basic Java, test', '2020-01-20', '2020-01-20', 1 );



INSERT INTO sunbird_courses.user_enrolments (userid, courseid, batchid, active, completionpercentage, progress, status, enrolled_date, issued_certificates) VALUES ('user-001', 'do_1130928636168192001667', 'batch-001', False, 10, 0, 1, '2019-11-16 05:41:50', [{'identifier': '9b72c1d7-a639-4b4b-a06e-24e2eb4b3f86', 'lastIssuedOn': '2022-02-02T10:55:59.411+0000', 'name': 'Certification of  completion', 'token': 'Y1X4J4'}]);

INSERT INTO sunbird_courses.user_enrolments (userid, courseid, batchid, active, completionpercentage, progress, status, enrolled_date, issued_certificates) VALUES ('user-002', 'do_1130928636168192001667', 'batch-001', True, 15, 1, 1, '2019-11-15 05:41:50', [{'identifier': '8b72c1d7-a639-4b4b-a06e-24e2eb4b3f86', 'lastIssuedOn': '2022-02-02T10:55:59.411+0000', 'name': 'Certification of  completion', 'token': 'Y1X4J4'}]);

INSERT INTO sunbird_courses.user_enrolments (userid, courseid, batchid, active, completionpercentage, progress, status, enrolleddate) VALUES ('user-003', 'do_1130928636168192001667', 'batch-001', True, 20, 1, 1, '2019-11-15 05:41:50:382+0000');
INSERT INTO sunbird_courses.user_enrolments (userid, courseid, batchid, active, completionpercentage, progress, status, enrolleddate) VALUES ('user-004',
 'do_1130928636168192001667', 'batch-001', True, 10, 0, 1, '2019-11-15 05:41:50:382+0000');


INSERT INTO sunbird_courses.user_enrolments (userid, courseid, batchid, active, completionpercentage, progress, status, enrolleddate) VALUES ('user-004',
 'do_1130928636168192001668', 'batch-002', True, 10, 0, 1, '2019-11-15 05:41:50:382+0000');





INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on, last_attempted_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-001', 'do_112876961957437440179', 'attempat-001', '20', 20, 20, toTimeStamp(toDate(now())), 1626327000000);
INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on, last_attempted_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-001', 'do_112876961957437440179', 'attempat-002', '20', 20, 20, toTimeStamp(toDate(now())), 1626327000000);
INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on, last_attempted_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-001', 'do_112876961957437440179', 'attempat-003', '20', 20, 20, toTimeStamp(toDate(now())), 1626327000000);
INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on, last_attempted_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-001', 'do_112876961957437440180', 'attempat-001', '20', 20, 20, toTimeStamp(toDate(now())), 1626327000000);

INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-002', 'do_112876961957437440179', 'attempt-001', '10', 10, 7, toTimeStamp(toDate(now())));

INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-002', 'do_112876961957437440179', 'attempt-002', '10', 10, 7, toTimeStamp(toDate(now())));


INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-003', 'do_112876961957437440179', 'attempt-001', '10', 10, 7, toTimeStamp(toDate(now())));

INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-003', 'do_112876961957437440179', 'attempt-002', '10', 10, 7, toTimeStamp(toDate(now())));

INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-003', 'do_112876961957437440179', 'attempt-003', '10', 10, 7, toTimeStamp(toDate(now())));

INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-003', 'do_112876961957437440179', 'attempt-004', '10', 10, 7, toTimeStamp(toDate(now())));

INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_1130928636168192001667', 'batch-001', 'user-004', 'do_112876961957437440179', 'attempt-001', '10', 10, 7, toTimeStamp(toDate(now())));


INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_1130928636168192001668', 'batch-002', 'user-004', 'do_112876961957437440179', 'attempt-001', '10', 10, 7, toTimeStamp(toDate(now())));






INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_11306040245271756813015', 'batch-001', 'user-008', 'do_112876961957437440179', 'attempt-001', '10', 10, 10, toTimeStamp(toDate(now())));
INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_11306040245271756813015', 'batch-001', 'user-010', 'do_11307593493010022418', 'attempt-001', '15', 15, 15, toTimeStamp(toDate(now())));
INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, updated_on) VALUES ('do_112835334818643968148', 'batch-004', 'user-014', 'do_11307593493010022418', 'attempt-001', '15', 15, 15, toTimeStamp(toDate(now())));



INSERT INTO sunbird.system_settings (id, field, value) VALUES ('custodianOrgId', 'custodianOrgId', '0130107621805015045');