# Testing Cursor Repository

This is a test repository for cursor development and testing purposes.

## Features
- Basic repository setup
- README documentation
- Ready for development
- **DTP-470 Implementation**: Java class with main method that prints "hello Rahul"

## DTP-470 Implementation

### Requirements
- Create a Java class with a main method
- The main method should print "hello Rahul"

### Implementation
The requirement has been implemented in `src/main/java/com/boomerang/HelloRahul.java`

```java
public class HelloRahul {
    public static void main(String[] args) {
        System.out.println("hello Rahul");
    }
}
```

### How to Run
1. **Using Maven**:
   ```bash
   mvn compile
   mvn exec:java -Dexec.mainClass="com.boomerang.HelloRahul"
   ```

2. **Using Java directly**:
   ```bash
   javac src/main/java/com/boomerang/HelloRahul.java
   java -cp src/main/java com.boomerang.HelloRahul
   ```

3. **Using Maven JAR**:
   ```bash
   mvn package
   java -jar target/testing-cursor-1.0.0.jar
   ```

## Project Structure
```
testing-cursor/
├── src/
│   └── main/
│       └── java/
│           └── com/
│               └── boomerang/
│                   └── HelloRahul.java
├── pom.xml
└── README.md
```

## Getting Started
Clone this repository and start developing!

## Jira Reference
- **Issue**: DTP-470
- **Summary**: Testing MCP server
- **Description**: We need a java class with a main method that prints hello Rahul
- **Status**: Implemented ✅