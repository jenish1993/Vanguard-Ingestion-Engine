# Setup Guide

## Environment Prerequisites

### 1. Python (Version 3.10+)
- **macOS**: `brew install python@3.10`
- **Linux (Ubuntu/Debian)**: `sudo apt update && sudo apt install python3.10 python3.10-venv`
- **Windows**: Download the installer from the [official Python website](https://www.python.org/downloads/) and ensure you check "Add Python to PATH" during installation.

### 2. Java/JDK (Java 8 or Java 11 required by PySpark)
- **macOS**: `brew install openjdk@11`
  - After installing, link it and set `JAVA_HOME` (add this to your `~/.zshrc` or `~/.bash_profile`): 
    ```bash
    sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
    export JAVA_HOME=$(/usr/libexec/java_home -v 11)
    ```
- **Linux (Ubuntu/Debian)**: `sudo apt update && sudo apt install openjdk-11-jdk`
  - Set `JAVA_HOME` in your `~/.bashrc`: `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`
- **Windows**: Download the [Adoptium OpenJDK 11](https://adoptium.net/temurin/releases/?version=11) MSI installer. During installation, make sure to enable the "Set JAVA_HOME variable" feature.

## Virtual Environment Isolation

Create and activate a virtual environment to isolate the project dependencies.

### macOS/Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Windows
```bash
python -m venv .venv
.venv\Scripts\activate
```

## Basic Pip Installations

Once the virtual environment is activated, install `pyspark`. If you plan to connect to a specific database using JDBC (e.g., PostgreSQL or MySQL), you will also need to download the corresponding JDBC driver jar and place it in the correct PySpark jars folder or supply it during initialization.

```bash
pip install pyspark
```
