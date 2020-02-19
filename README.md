AWS Utils
=========

AWS Utilities.

# BUild
Build jar file.
``` 
gradle build
```

Publish artifacts into `build/repo`
``` 
gradle publish
```

Build and download runtime dependencies into `build/runtime`.
``` 
gradle runtime
```

To create distribution, first build and download dependencies.
```
gradle clean build runtime
```

Then create a zip file under `build/distributions`
```
gradle distZip
```

# Usage
Purge a S3 bucket. This supports versioning enabled bucket.
```
java com.purestorage.reddot.awsutil.PurgeBucket ap-southeast-1 mybucket 
```