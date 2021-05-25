#!/bin/bash

APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-common

java -cp "${APP_HOME}/target/streamingetl-common-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev1 com.transglobe.streamingetl.common.app.HealthCheckApp
