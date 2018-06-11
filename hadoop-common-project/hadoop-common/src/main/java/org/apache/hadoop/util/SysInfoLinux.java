/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plugin to calculate resource information on Linux systems.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SysInfoLinux extends SysInfo {
  private static final Logger LOG = LoggerFactory.getLogger(SysInfoLinux.class);

  /**
   * proc's meminfo virtual file has keys-values in the format
   * "key:[ \t]*value[ \t]kB".
   */
  private static final String PROCFS_MEMFILE = "/proc/meminfo";
  private static final Pattern PROCFS_MEMFILE_FORMAT =
      Pattern.compile("^([a-zA-Z_()]*):[ \t]*([0-9]*)[ \t]*(kB)?");

  // We need the values for the following keys in meminfo
  private static final String MEMTOTAL_STRING = "MemTotal";
  private static final String SWAPTOTAL_STRING = "SwapTotal";
  private static final String MEMFREE_STRING = "MemFree";
  private static final String SWAPFREE_STRING = "SwapFree";
  private static final String INACTIVE_STRING = "Inactive";
  private static final String INACTIVEFILE_STRING = "Inactive(file)";
  private static final String HARDWARECORRUPTED_STRING = "HardwareCorrupted";
  private static final String HUGEPAGESTOTAL_STRING = "HugePages_Total";
  private static final String HUGEPAGESIZE_STRING = "Hugepagesize";



  /**
   * Patterns for parsing /proc/cpuinfo.
   */
  private static final String PROCFS_CPUINFO = "/proc/cpuinfo";
  private static final Pattern PROCESSOR_FORMAT =
      Pattern.compile("^processor[ \t]:[ \t]*([0-9]*)");
  private static final Pattern FREQUENCY_FORMAT =
      Pattern.compile("^cpu MHz[ \t]*:[ \t]*([0-9.]*)");
  private static final Pattern PHYSICAL_ID_FORMAT =
      Pattern.compile("^physical id[ \t]*:[ \t]*([0-9]*)");
  private static final Pattern CORE_ID_FORMAT =
      Pattern.compile("^core id[ \t]*:[ \t]*([0-9]*)");

  /**
   * Pattern for parsing /proc/stat.
   */
  private static final String PROCFS_STAT = "/proc/stat";
  private static final Pattern CPU_TIME_FORMAT =
      Pattern.compile("^cpu[ \t]*([0-9]*)" +
                      "[ \t]*([0-9]*)[ \t]*([0-9]*)[ \t].*");
  private CpuTimeTracker cpuTimeTracker;

  /**
   * Pattern for parsing /proc/net/dev.
   */
  private static final String PROCFS_NETFILE = "/proc/net/dev";
  private static final Pattern PROCFS_NETFILE_FORMAT =
      Pattern .compile("^[ \t]*([a-zA-Z]+[0-9]*):" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+).*");

  /**
   * Pattern for parsing /proc/diskstats.
   */
  private static final String PROCFS_DISKSFILE = "/proc/diskstats";
  private static final Pattern PROCFS_DISKSFILE_FORMAT =
      Pattern.compile("^[ \t]*([0-9]+)[ \t]*([0-9 ]+)" +
              "(?!([a-zA-Z]+[0-9]+))([a-zA-Z]+)" +
              "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
              "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
              "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)");
  /**
   * Pattern for parsing /sys/block/partition_name/queue/hw_sector_size.
   */
  private static final Pattern PROCFS_DISKSECTORFILE_FORMAT =
      Pattern.compile("^([0-9]+)");


  public static final long REFRESH_INTERVAL_MS = 60 * 1000;

  private static final String REFRESH_GPU_INFO_CMD = "nvidia-smi";
  private static final String REFRESH_PORTS_CMD = "netstat -anlut";

  /**
   Wed Mar  7 08:28:10 2018
   +-----------------------------------------------------------------------------+
   | NVIDIA-SMI 384.111                Driver Version: 384.111                   |
   |-------------------------------+----------------------+----------------------+
   | GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
   | Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
   |===============================+======================+======================|
   |   0  Tesla K80           Off  | 00006B24:00:00.0 Off |                    0 |
   | N/A   26C    P8    34W / 149W |   3322MiB / 11439MiB |      0%      Default |
   +-------------------------------+----------------------+----------------------+
   |   1  Tesla K80           Off  | 000083D4:00:00.0 Off |                    1 |
   | N/A   32C    P8    28W / 149W |     11MiB / 11439MiB |      0%      Default |
   +-------------------------------+----------------------+----------------------+
   |   2  Tesla K80           Off  | 00009D9C:00:00.0 Off |                    0 |
   | N/A   29C    P8    25W / 149W |     12MiB / 11439MiB |      0%      Default |
   +-------------------------------+----------------------+----------------------+
   |   3  Tesla K80           Off  | 0000B6D4:00:00.0 Off |                  N/A |
   | N/A   24C    P8    35W / 149W |      1MiB / 11439MiB |      0%      Default |
   +-------------------------------+----------------------+----------------------+
   |   4  Tesla K80           Off  | 00009D9C:00:00.0 Off |                    0 |
   | N/A   29C    P8    25W / 149W |     12MiB / 11439MiB |      0%      Default |
   +-------------------------------+----------------------+----------------------+
   |   5  Tesla K80           Off  | 0000B6D4:00:00.0 Off |                  N/A |
   | N/A   24C    P8    35W / 149W |      1MiB / 11439MiB |      0%      Default |
   +-------------------------------+----------------------+----------------------+
   |   6  Tesla K80           Off  | 00009D9C:00:00.0 Off |                    0 |
   | N/A   29C    P8    25W / 149W |     12MiB / 11439MiB |      0%      Default |
   +-------------------------------+----------------------+----------------------+
   |   7  Tesla K80           Off  | 0000B6D4:00:00.0 Off |                    0 |
   | N/A   24C    P8    35W / 149W |      1MiB / 11439MiB |      0%      Default |
   +-------------------------------+----------------------+----------------------+

   +-----------------------------------------------------------------------------+
   | Processes:                                                       GPU Memory |
   |  GPU       PID   Type   Process name                             Usage      |
   |=============================================================================|
   |  0         11111  c     test_process_.bin                        400MiB     |
   |  2         12222  c     test_process_.bin                        401MiB     |
   |  3         14441  c     test_process_.bin                        402MiB     |
   |  4         11555  c     test_process_.bin                        403MiB     |
   |  7         11777  c     test_process_.bin                        405MiB     |
   +-----------------------------------------------------------------------------+
   */
  Pattern GPU_INFO_FORMAT =
      Pattern.compile("\\s+([0-9]{1,2})\\s+[\\s\\S]*\\s+(0|1|N/A)\\s+");
  Pattern GPU_MEM_FORMAT =
      Pattern.compile("([0-9]+)MiB\\s*/\\s*([0-9]+)MiB");

  Pattern GPU_PROCESS_FORMAT =
      Pattern.compile("\\s+([0-9]{1,2})\\s+[\\s\\S]*\\s+([0-9]+)MiB");
  /**
   * the output format of the Ports information:
   Proto Recv-Q Send-Q Local Address           Foreign Address         State
   tcp        0      0 0.0.0.0:10022           0.0.0.0:*               LISTEN
   tcp        0      0 10.0.3.4:38916          168.63.129.16:80        TIME_WAIT
   tcp        0      0 10.0.3.4:56822          52.226.8.57:443         TIME_WAIT
   tcp        0      0 10.0.3.4:38898          168.63.129.16:80        TIME_WAIT
   tcp        0      0 10.0.3.4:56828          52.226.8.57:443         TIME_WAIT
   */
  private static final Pattern PORTS_FORMAT =
      Pattern.compile(":([0-9]+)");


  private String procfsMemFile;
  private String procfsCpuFile;
  private String procfsStatFile;
  private String procfsNetFile;
  private String procfsDisksFile;
  private String procfsGpuFile;
  private String procfsPortsFile;
  private long jiffyLengthInMillis;

  private long ramSize = 0;
  private long swapSize = 0;
  private long ramSizeFree = 0;  // free ram space on the machine (kB)
  private long swapSizeFree = 0; // free swap space on the machine (kB)
  private long inactiveSize = 0; // inactive memory (kB)
  private long inactiveFileSize = -1; // inactive cache memory, -1 if not there
  private long hardwareCorruptSize = 0; // RAM corrupt and not available
  private long hugePagesTotal = 0; // # of hugepages reserved
  private long hugePageSize = 0; // # size of each hugepage

  private int numGPUs = 0; // number of GPUs on the system
  private Long gpuAttributeCapacity = 0L; // bit map of GPU utilization, 1 means free, 0 means occupied
  private Long gpuAttributeUsed = 0L; // bit map of GPU utilization, 1 means free, 0 means occupied
  private long lastRefreshGpuTime = 0L;
  private long lastRefreshPortsTime = 0L;
  private String usedPorts = "";


  /* number of logical processors on the system. */
  private int numProcessors = 0;
  /* number of physical cores on the system. */
  private int numCores = 0;
  private long cpuFrequency = 0L; // CPU frequency on the system (kHz)
  private long numNetBytesRead = 0L; // aggregated bytes read from network
  private long numNetBytesWritten = 0L; // aggregated bytes written to network
  private long numDisksBytesRead = 0L; // aggregated bytes read from disks
  private long numDisksBytesWritten = 0L; // aggregated bytes written to disks

  private boolean readMemInfoFile = false;
  private boolean readCpuInfoFile = false;

  /* map for every disk its sector size */
  private HashMap<String, Integer> perDiskSectorSize = null;

  public static final long PAGE_SIZE = getConf("PAGESIZE");
  public static final long JIFFY_LENGTH_IN_MILLIS =
      Math.max(Math.round(1000D / getConf("CLK_TCK")), -1);

  private static long getConf(String attr) {
    if(Shell.LINUX) {
      try {
        ShellCommandExecutor shellExecutorClk = new ShellCommandExecutor(
            new String[] {"getconf", attr });
        shellExecutorClk.execute();
        return Long.parseLong(shellExecutorClk.getOutput().replace("\n", ""));
      } catch (IOException|NumberFormatException e) {
        return -1;
      }
    }
    return -1;
  }

  /**
   * Get current time.
   * @return Unix time stamp in millisecond
   */
  long getCurrentTime() {
    return System.currentTimeMillis();
  }

  public SysInfoLinux() {
    this(PROCFS_MEMFILE, PROCFS_CPUINFO, PROCFS_STAT,
         PROCFS_NETFILE, PROCFS_DISKSFILE, null, null, JIFFY_LENGTH_IN_MILLIS);
  }

  /**
   * Constructor which allows assigning the /proc/ directories. This will be
   * used only in unit tests.
   * @param procfsMemFile fake file for /proc/meminfo
   * @param procfsCpuFile fake file for /proc/cpuinfo
   * @param procfsStatFile fake file for /proc/stat
   * @param procfsNetFile fake file for /proc/net/dev
   * @param procfsDisksFile fake file for /proc/diskstats
   * @param jiffyLengthInMillis fake jiffy length value
   */
  @VisibleForTesting
  public SysInfoLinux(String procfsMemFile,
                                       String procfsCpuFile,
                                       String procfsStatFile,
                                       String procfsNetFile,
                                       String procfsDisksFile,
                                       String procfsGpuFile,
                                       String procfsPortsFile,
                                       long jiffyLengthInMillis) {
    this.procfsMemFile = procfsMemFile;
    this.procfsCpuFile = procfsCpuFile;
    this.procfsStatFile = procfsStatFile;
    this.procfsNetFile = procfsNetFile;
    this.procfsDisksFile = procfsDisksFile;
    this.procfsGpuFile = procfsGpuFile;
    this.procfsPortsFile = procfsPortsFile;
    this.jiffyLengthInMillis = jiffyLengthInMillis;
    this.cpuTimeTracker = new CpuTimeTracker(jiffyLengthInMillis);
    this.perDiskSectorSize = new HashMap<String, Integer>();
  }

  /**
   * Read /proc/meminfo, parse and compute memory information only once.
   */
  private void readProcMemInfoFile() {
    readProcMemInfoFile(false);
  }

  /**
   * Read /proc/meminfo, parse and compute memory information.
   * @param readAgain if false, read only on the first time
   */
  private void readProcMemInfoFile(boolean readAgain) {

    if (readMemInfoFile && !readAgain) {
      return;
    }

    // Read "/proc/memInfo" file
    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsMemFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // shouldn't happen....
      LOG.warn("Couldn't read " + procfsMemFile
          + "; can't determine memory settings");
      return;
    }

    Matcher mat;

    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_MEMFILE_FORMAT.matcher(str);
        if (mat.find()) {
          if (mat.group(1).equals(MEMTOTAL_STRING)) {
            ramSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(SWAPTOTAL_STRING)) {
            swapSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(MEMFREE_STRING)) {
            ramSizeFree = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(SWAPFREE_STRING)) {
            swapSizeFree = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(INACTIVE_STRING)) {
            inactiveSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(INACTIVEFILE_STRING)) {
            inactiveFileSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(HARDWARECORRUPTED_STRING)) {
            hardwareCorruptSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(HUGEPAGESTOTAL_STRING)) {
            hugePagesTotal = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(HUGEPAGESIZE_STRING)) {
            hugePageSize = Long.parseLong(mat.group(2));
          }
        }
        str = in.readLine();
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }

    readMemInfoFile = true;
  }

  /**
   * Read /proc/cpuinfo, parse and calculate CPU information.
   */
  private void readProcCpuInfoFile() {
    // This directory needs to be read only once
    if (readCpuInfoFile) {
      return;
    }
    HashSet<String> coreIdSet = new HashSet<>();
    // Read "/proc/cpuinfo" file
    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsCpuFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // shouldn't happen....
      LOG.warn("Couldn't read " + procfsCpuFile + "; can't determine cpu info");
      return;
    }
    Matcher mat;
    try {
      numProcessors = 0;
      numCores = 1;
      String currentPhysicalId = "";
      String str = in.readLine();
      while (str != null) {
        mat = PROCESSOR_FORMAT.matcher(str);
        if (mat.find()) {
          numProcessors++;
        }
        mat = FREQUENCY_FORMAT.matcher(str);
        if (mat.find()) {
          cpuFrequency = (long)(Double.parseDouble(mat.group(1)) * 1000); // kHz
        }
        mat = PHYSICAL_ID_FORMAT.matcher(str);
        if (mat.find()) {
          currentPhysicalId = str;
        }
        mat = CORE_ID_FORMAT.matcher(str);
        if (mat.find()) {
          coreIdSet.add(currentPhysicalId + " " + str);
          numCores = coreIdSet.size();
        }
        str = in.readLine();
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }
    readCpuInfoFile = true;
  }

  /**
   * Read /proc/stat file, parse and calculate cumulative CPU.
   */
  private void readProcStatFile() {
    // Read "/proc/stat" file
    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsStatFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // shouldn't happen....
      return;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = CPU_TIME_FORMAT.matcher(str);
        if (mat.find()) {
          long uTime = Long.parseLong(mat.group(1));
          long nTime = Long.parseLong(mat.group(2));
          long sTime = Long.parseLong(mat.group(3));
          cpuTimeTracker.updateElapsedJiffies(
              BigInteger.valueOf(uTime + nTime + sTime),
              getCurrentTime());
          break;
        }
        str = in.readLine();
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }
  }

  /**
   * Read /proc/net/dev file, parse and calculate amount
   * of bytes read and written through the network.
   */
  private void readProcNetInfoFile() {

    numNetBytesRead = 0L;
    numNetBytesWritten = 0L;

    // Read "/proc/net/dev" file
    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsNetFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      return;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_NETFILE_FORMAT.matcher(str);
        if (mat.find()) {
          assert mat.groupCount() >= 16;

          // ignore loopback interfaces
          if (mat.group(1).equals("lo")) {
            str = in.readLine();
            continue;
          }
          numNetBytesRead += Long.parseLong(mat.group(2));
          numNetBytesWritten += Long.parseLong(mat.group(10));
        }
        str = in.readLine();
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }
  }

  /**
   * Read /proc/diskstats file, parse and calculate amount
   * of bytes read and written from/to disks.
   */
  private void readProcDisksInfoFile() {

    numDisksBytesRead = 0L;
    numDisksBytesWritten = 0L;

    // Read "/proc/diskstats" file
    BufferedReader in;
    try {
      in = new BufferedReader(new InputStreamReader(
            new FileInputStream(procfsDisksFile), Charset.forName("UTF-8")));
    } catch (FileNotFoundException f) {
      return;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_DISKSFILE_FORMAT.matcher(str);
        if (mat.find()) {
          String diskName = mat.group(4);
          assert diskName != null;
          // ignore loop or ram partitions
          if (diskName.contains("loop") || diskName.contains("ram")) {
            str = in.readLine();
            continue;
          }

          Integer sectorSize;
          synchronized (perDiskSectorSize) {
            sectorSize = perDiskSectorSize.get(diskName);
            if (null == sectorSize) {
              // retrieve sectorSize
              // if unavailable or error, assume 512
              sectorSize = readDiskBlockInformation(diskName, 512);
              perDiskSectorSize.put(diskName, sectorSize);
            }
          }

          String sectorsRead = mat.group(7);
          String sectorsWritten = mat.group(11);
          if (null == sectorsRead || null == sectorsWritten) {
            return;
          }
          numDisksBytesRead += Long.parseLong(sectorsRead) * sectorSize;
          numDisksBytesWritten += Long.parseLong(sectorsWritten) * sectorSize;
        }
        str = in.readLine();
      }
    } catch (IOException e) {
      LOG.warn("Error reading the stream " + procfsDisksFile, e);
    } finally {
      // Close the streams
      try {
        in.close();
      } catch (IOException e) {
        LOG.warn("Error closing the stream " + procfsDisksFile, e);
      }
    }
  }

  /**
   * Read /sys/block/diskName/queue/hw_sector_size file, parse and calculate
   * sector size for a specific disk.
   * @return sector size of specified disk, or defSector
   */
  int readDiskBlockInformation(String diskName, int defSector) {

    assert perDiskSectorSize != null && diskName != null;

    String procfsDiskSectorFile =
            "/sys/block/" + diskName + "/queue/hw_sector_size";

    BufferedReader in;
    try {
      in = new BufferedReader(new InputStreamReader(
            new FileInputStream(procfsDiskSectorFile),
              Charset.forName("UTF-8")));
    } catch (FileNotFoundException f) {
      return defSector;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_DISKSECTORFILE_FORMAT.matcher(str);
        if (mat.find()) {
          String secSize = mat.group(1);
          if (secSize != null) {
            return Integer.parseInt(secSize);
          }
        }
        str = in.readLine();
      }
      return defSector;
    } catch (IOException|NumberFormatException e) {
      LOG.warn("Error reading the stream " + procfsDiskSectorFile, e);
      return defSector;
    } finally {
      // Close the streams
      try {
        in.close();
      } catch (IOException e) {
        LOG.warn("Error closing the stream " + procfsDiskSectorFile, e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public long getPhysicalMemorySize() {
    readProcMemInfoFile();
    return (ramSize
            - hardwareCorruptSize
            - (hugePagesTotal * hugePageSize)) * 1024;
  }

  /** {@inheritDoc} */
  @Override
  public long getVirtualMemorySize() {
    return getPhysicalMemorySize() + (swapSize * 1024);
  }

  /** {@inheritDoc} */
  @Override
  public long getAvailablePhysicalMemorySize() {
    readProcMemInfoFile(true);
    long inactive = inactiveFileSize != -1
        ? inactiveFileSize
        : inactiveSize;
    return (ramSizeFree + inactive) * 1024;
  }

  /** {@inheritDoc} */
  @Override
  public long getAvailableVirtualMemorySize() {
    return getAvailablePhysicalMemorySize() + (swapSizeFree * 1024);
  }

  /** {@inheritDoc} */
  @Override
  public int getNumProcessors() {
    readProcCpuInfoFile();
    return numProcessors;
  }

  /** {@inheritDoc} */
  @Override
  public int getNumCores() {
    readProcCpuInfoFile();
    return numCores;
  }

  /** {@inheritDoc} */
  @Override
  public long getCpuFrequency() {
    readProcCpuInfoFile();
    return cpuFrequency;
  }

  /** {@inheritDoc} */
  @Override
  public long getCumulativeCpuTime() {
    readProcStatFile();
    return cpuTimeTracker.getCumulativeCpuTime();
  }

  /** {@inheritDoc} */
  @Override
  public float getCpuUsagePercentage() {
    readProcStatFile();
    float overallCpuUsage = cpuTimeTracker.getCpuTrackerUsagePercent();
    if (overallCpuUsage != CpuTimeTracker.UNAVAILABLE) {
      overallCpuUsage = overallCpuUsage / getNumProcessors();
    }
    return overallCpuUsage;
  }

  /** {@inheritDoc} */
  @Override
  public float getNumVCoresUsed() {
    readProcStatFile();
    float overallVCoresUsage = cpuTimeTracker.getCpuTrackerUsagePercent();
    if (overallVCoresUsage != CpuTimeTracker.UNAVAILABLE) {
      overallVCoresUsage = overallVCoresUsage / 100F;
    }
    return overallVCoresUsage;
  }

  /** {@inheritDoc} */
  @Override
  public long getNetworkBytesRead() {
    readProcNetInfoFile();
    return numNetBytesRead;
  }

  /** {@inheritDoc} */
  @Override
  public long getNetworkBytesWritten() {
    readProcNetInfoFile();
    return numNetBytesWritten;
  }

  @Override
  public long getStorageBytesRead() {
    readProcDisksInfoFile();
    return numDisksBytesRead;
  }

  @Override
  public long getStorageBytesWritten() {
    readProcDisksInfoFile();
    return numDisksBytesWritten;
  }

  /** {@inheritDoc} */
  @Override
  public int getNumGPUs(boolean excludeOwnerlessUsingGpus, int gpuNotReadyMemoryThreshold) {
    refreshGpuIfNeeded(excludeOwnerlessUsingGpus, gpuNotReadyMemoryThreshold);
    return numGPUs;
  }

  /** {@inheritDoc} */
  @Override
  public long getGpuAttributeCapacity(boolean excludeOwnerlessUsingGpus, int gpuNotReadyMemoryThreshold) {
    refreshGpuIfNeeded(excludeOwnerlessUsingGpus, gpuNotReadyMemoryThreshold);
    return gpuAttributeCapacity;
  }

  @Override
  public String getPortsUsage() {
    refreshPortsIfNeeded();
    return usedPorts;
  }


  private InputStreamReader getInputGpuInfoStreamReader() throws Exception {
    if (procfsGpuFile == null) {
      Process pos = Runtime.getRuntime().exec(REFRESH_GPU_INFO_CMD);
      pos.waitFor();
      return new InputStreamReader(pos.getInputStream());
    } else {
      LOG.info("read GPU info from file:" + procfsGpuFile);
      return new InputStreamReader(
          new FileInputStream(procfsGpuFile), Charset.forName("UTF-8"));
    }
  }

  private void refreshGpuIfNeeded(boolean excludeOwnerlessUsingGpus, int gpuNotReadyMemoryThreshold) {

    long now = System.currentTimeMillis();
    if (now - lastRefreshGpuTime > REFRESH_INTERVAL_MS) {
      lastRefreshGpuTime = now;
      try {
        String ln = "";
        Long gpuAttributeUsed = 0L;
        Long gpuAttributeProcess = 0L;
        Long gpuAttributeCapacity = 0L;
        Map<String, String> usingMap = new HashMap<String, String>();

        Matcher mat = null;
        InputStreamReader ir = getInputGpuInfoStreamReader();
        BufferedReader input = new BufferedReader(ir);

        long currentIndex = 0;
        while ((ln = input.readLine()) != null) {
          mat = GPU_INFO_FORMAT.matcher(ln);
          if (mat.find()) {
            if (mat.group(1) != null && mat.group(2) != null) {
              long index = Long.parseLong(mat.group(1));
              currentIndex = index;

              String errCode = mat.group(2);
              if (!errCode.equals("1")) {
                gpuAttributeCapacity |= (1L << index);
              } else {
                LOG.error("ignored error: gpu " + index + " ECC code is 1, will make this gpu unavailable");
              }
            }
          }
          mat = GPU_MEM_FORMAT.matcher(ln);
          if (mat.find()) {
            if (mat.group(1) != null && mat.group(2) != null) {
              int usedMem = Integer.parseInt(mat.group(1));
              if (usedMem > gpuNotReadyMemoryThreshold) {
                gpuAttributeUsed |= (1L << currentIndex);
              }
            }
          }
          mat = GPU_PROCESS_FORMAT.matcher(ln);
          if (mat.find()) {
            if (mat.group(1) != null && mat.group(2) != null) {
              long index = Long.parseLong(mat.group(1));
              gpuAttributeProcess |= (1 << index);
            }
          }
        }
        input.close();
        ir.close();
        Long ownerLessGpus = (gpuAttributeUsed & ~gpuAttributeProcess);
        if ((ownerLessGpus != 0)) {
          LOG.info("GpuAttributeCapacity:" + Long.toBinaryString(gpuAttributeCapacity) + " GpuAttributeUsed:" + Long.toBinaryString(gpuAttributeUsed) + " GpuAttributeProcess:" + Long.toBinaryString(gpuAttributeProcess));
          if (excludeOwnerlessUsingGpus) {
            gpuAttributeCapacity = (gpuAttributeCapacity & ~ownerLessGpus);
            LOG.error("GPU:" + Long.toBinaryString(ownerLessGpus) + " is using by unknown process, will exclude these Gpus and won't schedule jobs into these Gpus");
          } else {
            LOG.error("GPU: " + Long.toBinaryString(ownerLessGpus) + " is using by unknown process, will ignore it and schedule jobs on these GPU. ");
          }
        }
        numGPUs = Long.bitCount(gpuAttributeCapacity);
        this.gpuAttributeCapacity = gpuAttributeCapacity;
        this.gpuAttributeUsed = gpuAttributeUsed;

      } catch (Exception e) {
        LOG.warn("error get GPU status info:" + e.toString());
      }
    }
  }

  private InputStreamReader getInputPortsStreamReader(String cmdLine) throws Exception {
    if (procfsPortsFile == null) {
      Process pos = Runtime.getRuntime().exec(cmdLine);
      pos.waitFor();
      return new InputStreamReader(pos.getInputStream());

    } else {
      LOG.info("read Ports info from file:" + procfsPortsFile);
      return new InputStreamReader(
          new FileInputStream(procfsPortsFile), Charset.forName("UTF-8"));
    }
  }

  private void refreshPortsIfNeeded() {

    long now = System.currentTimeMillis();
    if (now - lastRefreshPortsTime > REFRESH_INTERVAL_MS) {
      lastRefreshPortsTime = now;
      try {
        InputStreamReader ir = getInputPortsStreamReader(REFRESH_PORTS_CMD);
        BufferedReader input = new BufferedReader(ir);
        String ln = "";
        Matcher mat = null;
        usedPorts = "";
        while ((ln = input.readLine()) != null) {
          mat = PORTS_FORMAT.matcher(ln);
          if (mat.find()) {
            String port = mat.group().substring(1);

            if (usedPorts.isEmpty()) {
              usedPorts = port;
            } else {
              usedPorts = usedPorts + "," + port;
            }
          }
        }
        input.close();
        ir.close();
      } catch (Exception e) {
        LOG.warn("error get Ports usage info:" + e.toString());
      }
    } else {
    }
  }

  /**
   * Test the {@link SysInfoLinux}.
   *
   * @param args - arguments to this calculator test
   */
  public static void main(String[] args) {
    SysInfoLinux plugin = new SysInfoLinux();
    System.out.println("Physical memory Size (bytes) : "
        + plugin.getPhysicalMemorySize());
    System.out.println("Total Virtual memory Size (bytes) : "
        + plugin.getVirtualMemorySize());
    System.out.println("Available Physical memory Size (bytes) : "
        + plugin.getAvailablePhysicalMemorySize());
    System.out.println("Total Available Virtual memory Size (bytes) : "
        + plugin.getAvailableVirtualMemorySize());
    System.out.println("Number of Processors : " + plugin.getNumProcessors());
    System.out.println("CPU frequency (kHz) : " + plugin.getCpuFrequency());
    System.out.println("Cumulative CPU time (ms) : " +
            plugin.getCumulativeCpuTime());
    System.out.println("Total network read (bytes) : "
            + plugin.getNetworkBytesRead());
    System.out.println("Total network written (bytes) : "
            + plugin.getNetworkBytesWritten());
    System.out.println("Total storage read (bytes) : "
            + plugin.getStorageBytesRead());
    System.out.println("Total storage written (bytes) : "
            + plugin.getStorageBytesWritten());

    System.out.println("Number of GPUs : " + plugin.getNumGPUs(true, 0));
    System.out.println("GPUs attribute : " + plugin.getGpuAttributeCapacity(true, 0));
    System.out.println("used Ports : " + plugin.getPortsUsage());

    try {
      // Sleep so we can compute the CPU usage
      Thread.sleep(500L);
    } catch (InterruptedException e) {
      // do nothing
    }
    System.out.println("CPU usage % : " + plugin.getCpuUsagePercentage());
  }

  @VisibleForTesting
  void setReadCpuInfoFile(boolean readCpuInfoFileValue) {
    this.readCpuInfoFile = readCpuInfoFileValue;
  }

  public long getJiffyLengthInMillis() {
    return this.jiffyLengthInMillis;
  }
}
