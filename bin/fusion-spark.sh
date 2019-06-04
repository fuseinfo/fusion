#!/usr/bin/env bash
cd $(dirname "$0")/..
export FUSION_HOME=$PWD

QUEUE_NAME=${QUEUE_NAME:-default}
APP_NAME=${APP_NAME:-FUSION}
MASTER=${MASTER:-local}
EXEC_CORE=${EXEC_CORE:-1}
EXEC_MEM=${EXEC_MEM:-2G}
NUM_EXEC=${NUM_EXEC:-1}
FUSION_CONF=${FUSION_CONF:-$FUSION_HOME/conf}


FUSION_JAR=$(ls $FUSION_HOME/lib/fusion-core_*.jar)
JARS_ALL=$(files=($FUSION_HOME/lib/*.*);files=("${files[@]/$FUSION_JAR}");IFS=,; echo "${files[*]}")
JARS=`echo $JARS_ALL|sed 's/,,/,/'`

CONF_JAR=/tmp/fusion_conf_$PPID.jar
if [ -n "$(ls -A $FUSION_CONF)" ]
then
  cd $FUSION_CONF
  jar cf $CONF_JAR *
  chmod 700 $CONF_JAR
  JARS=$JARS,$CONF_JAR
  cd -
fi

if [ "$SPARK_HOME" == "" ]
then
  SPARK_CMD=spark-submit
else
  SPARK_CMD=$SPARK_HOME/bin/spark-submit
fi

if [ "$PRINCIPAL" != "" ] && [ "$KEYTAB" != "" ]
then
  SECURITY="--principal $PRINCIPAL --keytab $KEYTAB"
fi

if [ "$JAAS_CONF" != "" ]
then
  EXTRA_EXEC_OPTS="--conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=$JAAS_CONF $EXTRA_JAVA_OPTS"
  EXTRA_DRIV_OPTS="--conf spark.driver.extraJavaOptions=-Djava.security.auth.login.config=$JAAS_CONF $EXTRA_JAVA_OPTS"
  JARS=$JARS,$FUSION_HOME/$JAAS_CONF
  if [ "$JAAS_KEYTAB" != "" ]
  then
    JARS=$JARS,$FUSION_HOME/$JAAS_KEYTAB
  fi
  if [ "$TRUST_STORE" != "" ]
  then
    JARS=$JARS,$FUSION_HOME/$TRUST_STORE
  fi
else
  if [ "$EXTRA_JAVA_OPTS" != "" ]
  then
    EXTRA_EXEC_OPTS="--conf spark.executor.extraJavaOptions=$EXTRA_JAVA_OPTS"
    EXTRA_DRIV_OPTS="--conf spark.driver.extraJavaOptions=$EXTRA_JAVA_OPTS"
  fi
fi

if [ "$ACLS" != "" ]
then
  CONF_ACLS="--conf spark.ui.view.acls=$ACLS"
fi

if [ "MASTER" == "yarn-cluster" ]
then
  FUSION_OUTPUT=/tmp/${USER}_${PPID}.out
  exec $SPARK_CMD --class com.fuseinfo.fusion.Fusion --master $MASTER --name $APP_NAME --executor-memory $EXEC_MEM \
  --executor-cores $EXEC_CORE --queue $QUEUE_NAME $CONF_ACLS --conf "spark.driver.userClassPathFirst=true" \
  --conf "spark.executor.userClassPathFirst=true" $EXTRA_EXEC_OPTS $EXTRA_DRIV_OPTS $FUSION_EXTRA_CONF $SECURITY \
  --num-executors $NUM_EXEC --jars $JARS $FUSION_JAR "$@" > $FUSION_OUTPUT 2>&1 & FUSION_PID=$!
  sleep 10
  grep 'state: RUNNING' $FUSION_OUTPUT
  until [ $? -eq 0 ]
  do
    if ps -p $FUSION_PID
    then
      sleep 10
      grep 'state: RUNNING' $FUSION_OUTPUT
    else
      cat $FUSION_OUTPUT
      echo "Failed to launch FUSION"
      rm $CONF_JAR
      exit 1
     fi
  done
  kill -9 $FUSION_PID
  echo "Fusion was launched successfully."
  rm $FUSION_OUTPUT
else
  exec $SPARK_CMD --class com.fuseinfo.fusion.Fusion --master $MASTER --name $APP_NAME --executor-memory $EXEC_MEM \
  --executor-cores $EXEC_CORE --queue $QUEUE_NAME $CONF_ACLS --conf "spark.driver.userClassPathFirst=true" \
  --conf "spark.executor.userClassPathFirst=true" $EXTRA_EXEC_OPTS $EXTRA_DRIV_OPTS $FUSION_EXTRA_CONF $SECURITY \
  --num-executors $NUM_EXEC --jars $JARS $FUSION_JAR "$@"
fi
rm $CONF_JAR
