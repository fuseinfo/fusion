@ECHO OFF
PUSHD %~dp0..
SET FUSION_HOME=%CD%

IF "x%SPARK_HOME%" == "x" (
  SET SPARK_CMD=spark-submit.cmd
) ELSE (
  SET SPARK_CMD=%SPARK_HOME%\bin\spark-submit.cmd
)

IF NOT DEFINED MASTER (SET MASTER=local[*])

IF NOT DEFINED QUEUE_NAME (SET QUEUE_NAME=default)

IF NOT DEFINED APP_NAM (SET APP_NAME=FUSION)

IF NOT DEFINED EXEC_CORE (SET EXEC_CORE=1)

IF NOT DEFINED EXEC_MEM (SET EXEC_MEM=2G)

IF NOT DEFINED NUM_EXEC (SET NUM_EXEC=1)

IF NOT DEFINED FUSION_CONF (SET FUSION_CONF=%FUSION_HOME%\conf)

CD lib
SET JARS=
DEL %TMP%\fusion_jars
FOR /R %%i in (*) do (echo SET JARS=%%JARS%%lib\%%~nxi,>> %TMP%\fusion_jars)
TYPE %TMP%\fusion_jars | FINDSTR /v fusion-core_ > %TMP%\fusion_jars.bat
CALL %TMP%\fusion_jars.bat
DEL %TMP%\fusion_jars.bat %TMP%\fusion_jars

CD %FUSION_CONF%
%JAVA_HOME%\bin\jar cf %TMP%\fusion_conf.jar *
IF ERRORLEVEL 1 GOTO NEXT
SET JARS=%JARS%%TMP%\fusion_conf.jar
:NEXT

CD ..\lib
FOR /R %%i in (fusion-core_*.jar) DO SET FUSION_JAR=%%i


IF NOT "x%PRINCIPAL%" == "x" IF NOT "x%KEYTAB%" == "x" SET SECURITY=--principal %PRINCIPAL% --keytab %KEYTAB%

IF NOT "x%JAAS_CONF%" == "x" (
  SET EXEC_OPTS=--conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=%JAAS_CONF% %EXTRA_JAVA_OPTS%
  SET DRIV_OPTS=--conf spark.driver.extraJavaOptions=-Djava.security.auth.login.config=%JAAS_CONF% %EXTRA_JAVA_OPTS%
  SET JARS=%JARS%,%FUSION_HOME%\%JAAS_CONF%
  IF  NOT "x%JAAS_KEYTAB%" == "x" (SET JARS=$JARS,%FUSION_HOME%\%JAAS_KEYTA%)
  IF NOT "x%TRUST_STORE%" == "x" (SET JARS=$JARS,%FUSION_HOME%\%TRUST_STORE%)
)

IF "%MASTER:~0,5%" == "local" (SET WAREHOUSE=--conf spark.sql.warehouse.dir=%FUSION_HOME%) ELSE (SET WAREHOUSE=)

cd %FUSION_HOME%
CALL %SPARK_CMD% --class com.fuseinfo.fusion.Fusion --master %MASTER% --name %APP_NAME% --executor-memory %EXEC_MEM%^
 --executor-cores %EXEC_CORE% --queue %QUEUE_NAME% --conf spark.driver.userClassPathFirst=true^
 --conf spark.executor.userClassPathFirst=true %EXEC_OPTS% %DRIV_OPTS% %FUSION_EXTRA_CONF% %WAREHOUSE% %SECURITY%^
 --num-executors %NUM_EXEC% --jars %JARS% %FUSION_JAR% %*

DEL %TMP%\fusion_conf.jar
POPD