# um-console-subscriber
Prints events as JSON to console

## Build requirements:
 - Maven
 - Java 1.8
 - Access to nClient.jar maven repo (e.g., private nexus server), can add to your local with the following command:
 
 `mvn install:install-file -Dfile=nClient.jar -DgroupId=com.softwareag -DartifactId=nClient -Dversion=9.12.0 -Dpackaging=jar -DgeneratePom=true`

## Usage:

`java -jar %jar_file% %parameters%`

### Parameters
  - `rname` -> the UM uri (e.g., nsp://host:port), at least one of these is needed, can specify the matching one for each channel to subscribe to multiple instances at the same time
  - `channel` -> the name of the channel to subscribe, at least one of these is needed
  - `filter` -> optional, applies selector when creating the subscription, see [UM Filtering reference](http://um.terracotta.org/#page/%2Fum.terracotta.org%2Funiversal-messaging-webhelp%2Fto-nadvancedfiltering.html)
  - `start` -> optional, messsage id to start with

### Example:

`java -jar um-console-subscriber-0.1.1-RELEASE.jar --rname=nsp://uslx416:9000 --channel=/wm/is/AmGlDocs/Order/OrderUDM --channel=/wm/is/AmGlDocs/Order/ReturnUDM`
