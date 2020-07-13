# Laminar Maven resources

Laminar's embeddable libraries are available here:

```
<repositories>
	<repository>
		<id>laminar-repo</id>
		<url>https://github.com/jmdisher/Laminar/raw/maven/repo</url>
		<releases>
			<enabled>true</enabled>
		</releases>
		<snapshots>
			<enabled>true</enabled>
		</snapshots>
	</repository>
</repositories>
```

Currently, this is just the clientlib for the `0.0-research1` build:

```
<dependencies>
	<dependency>
		<groupId>com.jeffdisher.laminar</groupId>
		<artifactId>clientlib</artifactId>
		<version>0.0-research1</version>
	</dependency>
</dependencies>
```

