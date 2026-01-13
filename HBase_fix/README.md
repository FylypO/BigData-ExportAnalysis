# Node1 VM Configuration & Repair Guide

Follow these steps to fix the scripts and configuration on the **node1** Virtual Machine.

## 1. Update Scripts and Configuration

Perform the following steps to backup existing scripts and replace them with the correct versions from the repository.

### Step 1: Update `bootstrap.sh`
```bash
sudo cp /vagrant/scripts/bootstrap.sh /vagrant/scripts/bootstrap.sh.backup
sudo rm /vagrant/scripts/bootstrap.sh
sudo nano /vagrant/scripts/bootstrap.sh
# PASTE the content of the 'bootstrap.sh' file from the repository here, then save and exit (Ctrl+O, Enter, Ctrl+X)
```

### Step 2: Update `start-hbase.sh`
```bash
sudo cp /vagrant/scripts/start-hbase.sh /vagrant/scripts/start-hbase.sh.backup
sudo rm /vagrant/scripts/start-hbase.sh
sudo nano /vagrant/scripts/start-hbase.sh
# PASTE the content of the 'start-hbase.sh' file from the repository here
```

### Step 3: Create `full-restart.sh`
```bash
sudo nano /vagrant/scripts/full-restart.sh
# PASTE the content of the 'full-restart.sh' file from the repository here
```

### Step 4: Set Executable Permissions

```bash
sudo chmod +x /vagrant/scripts/bootstrap.sh
sudo chmod +x /vagrant/scripts/start-hbase.sh
sudo chmod +x /vagrant/scripts/full-restart.sh
```

### Step 5: Update HBase Configuration
```bash
sudo nano /usr/local/hbase/conf/hbase-site.xml
# PASTE the content of the 'hbase-site.xml' file from the repository here
```

## 2. Running the Services

Use the appropriate command depending on the state of your Virtual Machine.

- Initial Setup or Crash Recovery

    Run this command only after the first VM boot or if the services have crashed/failed:

    ```bash
    sudo /vagrant/scripts/full-restart.sh
    ```

- Routine Startup

    Run this command for standard startups:
    ```bash
    sudo /vagrant/scripts/bootstrap.sh
    ```