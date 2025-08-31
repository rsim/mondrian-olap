CREATE LOGIN mondrian_test WITH PASSWORD = 'mondrian_test', CHECK_POLICY = OFF
GO
ALTER SERVER ROLE [dbcreator] ADD MEMBER [mondrian_test]
GO
ALTER SERVER ROLE [diskadmin] ADD MEMBER [mondrian_test]
GO
ALTER SERVER ROLE [processadmin] ADD MEMBER [mondrian_test]
GO
ALTER SERVER ROLE [securityadmin] ADD MEMBER [mondrian_test]
GO
ALTER SERVER ROLE [serveradmin] ADD MEMBER [mondrian_test]
GO
ALTER SERVER ROLE [setupadmin] ADD MEMBER [mondrian_test]
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [mondrian_test]
GO
