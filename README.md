# data-model-migration
Tool for migrating PASS data from version 2.3 to 3.2

The tool supports all system properties/environment variables used in the [java-fedora-client](https://github.com/OA-PASS/java-fedora-client). Here is an example of how to run the migration tool: 
```
java -Dpass.fedora.baseurl="http://localhost:8080/fcrepo/rest" 
     -Dpass.elasticsearch.url="http://localhost:9200/pass" 
     -jar ./data-model-migration-0.0.2-SNAPSHOT-shaded.jar > 20181113_migration.log
```
