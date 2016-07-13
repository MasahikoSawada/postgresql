--
-- SUBSCRIPTION
--

-- fail - no publications
CREATE SUBSCRIPTION testsub CONNECTION 'foo';

-- fail - no connection
CREATE SUBSCRIPTION testsub PUBLICATION foo;

CREATE SUBSCRIPTION testsub PUBLICATION testpub CONNECTION 'testconn' INITIALLY DISABLED NOCREATE_SLOT;

\dRs+

ALTER SUBSCRIPTION testsub PUBLICATION testpub2, testpub3;

\dRs

ALTER SUBSCRIPTION testsub PUBLICATION testpub, testpub1 CONNECTION 'conn2';

\dRs+

BEGIN;
ALTER SUBSCRIPTION testsub ENABLE;

\dRs

ALTER SUBSCRIPTION testsub DISABLE;

\dRs

COMMIT;

SET client_min_messages TO 'error';
DROP SUBSCRIPTION testsub;
RESET client_min_messages;
