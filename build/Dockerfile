FROM jeanblanchard/java:8
MAINTAINER chenshuai <casel.chen@vpon.com>

RUN apk add --update bash
RUN mkdir /app

WORKDIR /

VOLUME ["/conf", "/logs"]

ADD *-assembly-*.jar /app/ssp-dedup-assembly.jar

ENTRYPOINT ["java", "-server", "-Xmx2G", "-XX:MaxPermSize=100m", "-XX:+UseConcMarkSweepGC", "-XX:+DisableExplicitGC", "-Dlogback.configurationFile=/conf/logback.xml", "-DPORT=2661", "-cp", "/app/ssp-dedup-assembly.jar", "com.vpon.ssp.report.dedup.Main", "-h", "12661", "-c", "/conf/application.conf"]
