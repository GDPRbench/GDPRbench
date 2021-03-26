/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.generator.UniformLongGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.util.*;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.
 * <p>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>minfieldlength</b>: the minimum size of each field (default: 1)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>readmetapurposeproportion</b>: what proportion of operations should be reads (default: 0.05)
 * <LI><b>readmetauserproportion</b>: what proportion of operations should be reads (default: 0.05)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>updatemetapurposeproportion</b>: what proportion of operations should be updates (default: 0.01)
 * <LI><b>updatemetauserproportion</b>: what proportion of operations should be updates (default: 0.01)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record,
 * modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate
 * on - uniform, zipfian, hotspot, sequential, exponential or latest (default: uniform)
 * <LI><b>minscanlength</b>: for scans, what is the minimum number of records to scan (default: 1)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the
 * number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting record for this
 * YCSB instance (default: 0)
 * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of records for this
 * YCSB instance (default: recordcount)
 * <LI><b>zeropadding</b>: for generating a record sequence compatible with string sort order by
 * 0 padding the record number. Controls the number of 0s to use for padding. (default: 1)
 * For example for row 5, with zeropadding=1 you get 'user5' key and with zeropading=8 you get
 * 'user00000005' key. In order to see its impact, zeropadding needs to be bigger than number of
 * digits in the record number.
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed
 * order ("hashed") (default: hashed)
 * <LI><b>fieldnameprefix</b>: what should be a prefix for field names, the shorter may decrease the
 * required storage size (default: "field")
 * </ul>
 */
public class GDPRWorkload extends Workload {
  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

  protected String table;

  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";
  
  private List<String> fieldnames;

  private List<String>[] fieldvalues;
  /**
   * The name of the property for the field length distribution. Options are "uniform", "zipfian"
   * (favouring short records), "constant", and "histogram".
   * <p>
   * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
   * fieldlength property. If "histogram", then the histogram will be read from the filename
   * specified in the "fieldlengthhistogram" property.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

  /**
   * The default field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

  /**
   * The default maximum length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

  /**
   * The name of the property for the minimum length of a field in bytes.
   */
  public static final String MIN_FIELD_LENGTH_PROPERTY = "minfieldlength";

  /**
   * The default minimum length of a field in bytes.
   */
  public static final String MIN_FIELD_LENGTH_PROPERTY_DEFAULT = "1";

  /**
   * The name of a property that specifies the filename containing the field length histogram (only
   * used if fieldlengthdistribution is "histogram").
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

  /**
   * The default filename containing a field length histogram.
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

  /**
   * Generator object that produces field lengths.  The value of this depends on the properties that
   * start with "FIELD_LENGTH_".
   */
  protected NumberGenerator fieldlengthgenerator;

  /**
   * The name of the property for deciding whether to read one field (false) or all fields (true) of
   * a record.
   */
  public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

  /**
   * The default value for the readallfields property.
   */
  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

  protected boolean readallfields;
  
  /**
   * The name of the property for deciding whether to write one field (false) or all fields (true)
   * of a record.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

  /**
   * The default value for the writeallfields property.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

  protected boolean writeallfields;

  public static final String READ_LOG_PROPERTY = "readlog";

  public static final String READ_LOG_PROPERTY_DEFAULT = "true";

  protected boolean readlog;
  
  public static final String CHECK_COMPL_PROPERTY = "checkcompliance";

  public static final String CHECK_COMPL_PROPERTY_DEFAULT = "true";

  protected boolean checkcompliance;
  
  public static final String PURPOSE_COUNT_PROPERTY = "purcount";

  public static final String PURPOSE_COUNT_PROPERTY_DEFAULT = "100";

  public static final String USER_COUNT_PROPERTY = "usrcount";

  public static final String USER_COUNT_PROPERTY_DEFAULT = "10000";

  public static final String OBJECTIVE_COUNT_PROPERTY = "objcount";

  public static final String OBJECTIVE_COUNT_PROPERTY_DEFAULT = "100";

  public static final String DECISION_COUNT_PROPERTY = "deccount";

  public static final String DECISION_COUNT_PROPERTY_DEFAULT = "2";

  public static final String ACL_COUNT_PROPERTY = "aclcount";

  public static final String ACL_COUNT_PROPERTY_DEFAULT = "10";

  public static final String SHARED_COUNT_PROPERTY = "shrcount";

  public static final String SHARED_COUNT_PROPERTY_DEFAULT = "10";

  public static final String SOURCE_COUNT_PROPERTY = "srccount";

  public static final String SOURCE_COUNT_PROPERTY_DEFAULT = "10";

  public static final String CATEGORY_COUNT_PROPERTY = "catcount";

  public static final String CATEGORY_COUNT_PROPERTY_DEFAULT = "10";

  /**
   * The name of the property for deciding whether to check all returned
   * data against the formation template to ensure data integrity.
   */
  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

  /**
   * The default value for the dataintegrity property.
   */
  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

  /**
   * Set to true if want to check correctness of reads. Must also
   * be set to true during loading phase to function.
   */
  private boolean dataintegrity;

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY = "readproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READMETA_PURPOSE_PROPORTION_PROPERTY = "readmetapurposeproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READMETA_PURPOSE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READMETA_USER_PROPORTION_PROPERTY = "readmetauserproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READMETA_USER_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are updates.
   */
  public static final String UPDATEMETA_PURPOSE_PROPORTION_PROPERTY = "updatemetapurposeproportion";

  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATEMETA_PURPOSE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are updates.
   */
  public static final String UPDATEMETA_USER_PROPORTION_PROPERTY = "updatemetauserproportion";

  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATEMETA_USER_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

  /**
   * The default proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

  /**
   * The default proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are read-modify-write.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

  /**
   * The default proportion of transactions that are scans.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are deletes.
   */
  public static final String DELETE_PROPORTION_PROPERTY = "deleteproportion";

  /**
   * The default proportion of transactions that are deletes.
   */
  public static final String DELETE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are deletes.
   */
  public static final String DELETEMETA_PURPOSE_PROPORTION_PROPERTY = "deletemetapurposeproportion";

  /**
   * The default proportion of transactions that are deletes.
   */
  public static final String DELETEMETA_PURPOSE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are deletes.
   */
  public static final String DELETEMETA_USER_PROPORTION_PROPERTY = "deletemetauserproportion";

  /**
   * The default proportion of transactions that are deletes.
   */
  public static final String DELETEMETA_USER_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the the distribution of requests across the keyspace. Options are
   * "uniform", "zipfian" and "latest"
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

  /**
   * The default distribution of requests across the keyspace.
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for adding zero padding to record numbers in order to match
   * string sort order. Controls the number of 0s to left pad with.
   */
  public static final String ZERO_PADDING_PROPERTY = "zeropadding";

  /**
   * The default zero padding value. Matches integer sort order
   */
  public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";


  /**
   * The name of the property for the min scan length (number of records).
   */
  public static final String MIN_SCAN_LENGTH_PROPERTY = "minscanlength";

  /**
   * The default min scan length.
   */
  public static final String MIN_SCAN_LENGTH_PROPERTY_DEFAULT = "1";

  /**
   * The name of the property for the max scan length (number of records).
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

  /**
   * The default max scan length.
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

  /**
   * The name of the property for the scan length distribution. Options are "uniform" and "zipfian"
   * (favoring short scans)
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

  /**
   * The default max scan length.
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the order to insert records. Options are "ordered" or "hashed"
   */
  public static final String INSERT_ORDER_PROPERTY = "insertorder";

  /**
   * Default insert order.
   */
  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

  /**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

  /**
   * Default value of the size of the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

  /**
   * Default value of the percentage operations accessing the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  /**
   * How many times to retry when insertion of a single item to a DB fails.
   */
  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

  /**
   * On average, how long to wait between the retries, in seconds.
   */
  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

  /**
   * Field name prefix.
   */
  public static final String FIELD_NAME_PREFIX = "fieldnameprefix";

  /**
   * Default value of the field name prefix.
   */
  public static final String FIELD_NAME_PREFIX_DEFAULT = "myfield";

  protected NumberGenerator keysequence;
  protected DiscreteGenerator operationchooser;
  protected NumberGenerator keychooser;
  protected NumberGenerator metadatachooser;
  protected NumberGenerator fieldchooser;
  protected AcknowledgedCounterGenerator transactioninsertkeysequence;
  protected NumberGenerator scanlength;
  protected boolean orderedinserts;
  protected long fieldcount;
  protected long recordcount;
  protected int zeropadding;
  protected int insertionRetryLimit;
  protected int insertionRetryInterval;

  private boolean isFirst = true;
  private Measurements measurements = Measurements.getMeasurements();

  protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
    NumberGenerator fieldlengthgenerator;
    String fieldlengthdistribution = p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
    int fieldlength =
        Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
    int minfieldlength =
        Integer.parseInt(p.getProperty(MIN_FIELD_LENGTH_PROPERTY, MIN_FIELD_LENGTH_PROPERTY_DEFAULT));
    String fieldlengthhistogram = p.getProperty(
        FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
    if (fieldlengthdistribution.compareTo("constant") == 0) {
      fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
    } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
      fieldlengthgenerator = new UniformLongGenerator(minfieldlength, fieldlength);
    } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
      fieldlengthgenerator = new ZipfianGenerator(minfieldlength, fieldlength);
    } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
      try {
        fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
      } catch (IOException e) {
        throw new WorkloadException(
            "Couldn't read field length histogram file: " + fieldlengthhistogram, e);
      }
    } else {
      throw new WorkloadException(
          "Unknown field length distribution \"" + fieldlengthdistribution + "\"");
    }
    return fieldlengthgenerator;
  }

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    fieldcount =
        Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    final String fieldnameprefix = p.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    populateValues(p);
    fieldlengthgenerator = GDPRWorkload.getFieldLengthGenerator(p);

    recordcount =
        Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    String requestdistrib =
        p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    int minscanlength =
        Integer.parseInt(p.getProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_PROPERTY_DEFAULT));
    int maxscanlength =
        Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    String scanlengthdistrib =
        p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    long insertstart =
        Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount=
        Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    // Confirm valid values for insertstart and insertcount in relation to recordcount
    if (recordcount < (insertstart + insertcount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }
    zeropadding =
        Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

    readallfields = Boolean.parseBoolean(
        p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
    writeallfields = Boolean.parseBoolean(
        p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

    readlog = Boolean.parseBoolean(
        p.getProperty(READ_LOG_PROPERTY, READ_LOG_PROPERTY_DEFAULT));
    checkcompliance = Boolean.parseBoolean(
        p.getProperty(CHECK_COMPL_PROPERTY, CHECK_COMPL_PROPERTY_DEFAULT));

    dataintegrity = Boolean.parseBoolean(
        p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY,
        FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      orderedinserts = false;
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else {
      orderedinserts = true;
    }

    keysequence = new CounterGenerator(insertstart);
    operationchooser = createOperationGenerator(p);

    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformLongGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the modulus over the
      // number of keys.
      // If the number of keys changes, this would shift the modulus, and we don't want that to
      // change which keys are popular so we'll actually construct the scrambled zipfian generator
      // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
      // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
      // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
      // the keyspace doesn't change from the perspective of the scrambled zipfian generator
      final double insertproportion = Double.parseDouble(
          p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
      int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor

      keychooser = new ScrambledZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys);
    } else if (requestdistrib.compareTo("latest") == 0) {
      keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(insertstart, insertstart + insertcount - 1,
          hotsetfraction, hotopnfraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }

    fieldchooser = new UniformLongGenerator(0, fieldcount - 1);
    metadatachooser = new UniformLongGenerator(1, 8);

    if (scanlengthdistrib.compareTo("uniform") == 0) {
      scanlength = new UniformLongGenerator(minscanlength, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      scanlength = new ZipfianGenerator(minscanlength, maxscanlength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
    }

    insertionRetryLimit = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
  }

  protected String buildKeyName(long keynum) {
    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }
    String value = Long.toString(keynum);
    int fill = zeropadding - value.length();
    String prekey = "key";
    for (int i = 0; i < fill; i++) {
      prekey += '0';
    }
    return prekey + value;
  }


/**
   * Fill values for all fields.
   */
  private void populateValues(final Properties p) {
    fieldnames = new ArrayList<>();
    fieldvalues = new ArrayList[10];
    int x = 0;
    for (int i = 0; i < fieldcount; i++) {
      //fieldnames.add(fieldnameprefix + i);
      switch(i) { 
      case 0: fieldnames.add("PUR"); 
        int purlength =
            Integer.parseInt(p.getProperty(PURPOSE_COUNT_PROPERTY, PURPOSE_COUNT_PROPERTY_DEFAULT));
        fieldvalues[i] = new ArrayList<String>();
        if (purlength <= 0) {
          purlength = Integer.parseInt(PURPOSE_COUNT_PROPERTY_DEFAULT);
        }
        for (x = 0; x < purlength; x++) {
          fieldvalues[i].add("purpose" + Integer.toString(x));
        }
        break;
      case 1: fieldnames.add("TTL");
        fieldvalues[i]=new ArrayList<>(
        Arrays.asList("30", "10000", "12000", "14000", "16000", "18000", "20000", "22000", "24000", "1000000"));
        break;
      case 2: fieldnames.add("USR"); 
        int usrlength =
            Integer.parseInt(p.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT));
        fieldvalues[i] = new ArrayList<String>();
        if (usrlength <= 0) {
          usrlength = Integer.parseInt(USER_COUNT_PROPERTY_DEFAULT);
        }
        for (x = 0; x < usrlength; x++) {
          fieldvalues[i].add("user" + Integer.toString(x));
        }
        break;
      case 3: fieldnames.add("OBJ"); 
        int objlength =
            Integer.parseInt(p.getProperty(OBJECTIVE_COUNT_PROPERTY, OBJECTIVE_COUNT_PROPERTY_DEFAULT));
        fieldvalues[i] = new ArrayList<String>();
        if (objlength <= 0) {
          objlength = Integer.parseInt(OBJECTIVE_COUNT_PROPERTY_DEFAULT);
        }
        for (x = 0; x < objlength; x++) {
          fieldvalues[i].add("obj" + Integer.toString(x));
        }
        break;
      case 4: fieldnames.add("DEC");
        int declength =
            Integer.parseInt(p.getProperty(DECISION_COUNT_PROPERTY, DECISION_COUNT_PROPERTY_DEFAULT));
        fieldvalues[i] = new ArrayList<String>();
        if (declength <= 0) {
          declength = Integer.parseInt(DECISION_COUNT_PROPERTY_DEFAULT);
        }
        for (x = 0; x < declength; x++) {
          fieldvalues[i].add("dec" + Integer.toString(x));
        }
        break;
      case 5: fieldnames.add("ACL"); 
        int acllength =
            Integer.parseInt(p.getProperty(ACL_COUNT_PROPERTY, ACL_COUNT_PROPERTY_DEFAULT));
        fieldvalues[i] = new ArrayList<String>();
        if (acllength <= 0) {
          acllength = Integer.parseInt(ACL_COUNT_PROPERTY_DEFAULT);
        }
        for (x = 0; x < acllength; x++) {
          fieldvalues[i].add("acl" + Integer.toString(x));
        }
        break;
      case 6: fieldnames.add("SHR");
        int shrlength =
            Integer.parseInt(p.getProperty(SHARED_COUNT_PROPERTY, SHARED_COUNT_PROPERTY_DEFAULT));
        fieldvalues[i] = new ArrayList<String>();
        if (shrlength <= 0) {
          shrlength = Integer.parseInt(SHARED_COUNT_PROPERTY_DEFAULT);
        }
        for (x = 0; x < shrlength; x++) {
          fieldvalues[i].add("shr" + Integer.toString(x));
        }
        break;
      case 7: fieldnames.add("SRC");
        int srclength =
            Integer.parseInt(p.getProperty(SOURCE_COUNT_PROPERTY, SOURCE_COUNT_PROPERTY_DEFAULT));
        fieldvalues[i] = new ArrayList<String>();
        if (srclength <= 0) {
          srclength = Integer.parseInt(SOURCE_COUNT_PROPERTY_DEFAULT);
        }
        for (x = 0; x < srclength; x++) {
          fieldvalues[i].add("src" + Integer.toString(x));
        }
        break;
      case 8: fieldnames.add("CAT");
        int catlength =
            Integer.parseInt(p.getProperty(CATEGORY_COUNT_PROPERTY, CATEGORY_COUNT_PROPERTY_DEFAULT));
        fieldvalues[i] = new ArrayList<String>();
        if (catlength <= 0) {
          catlength = Integer.parseInt(CATEGORY_COUNT_PROPERTY_DEFAULT);
        }
        for (x = 0; x < catlength; x++) {
          fieldvalues[i].add("cat" + Integer.toString(x));
        }
        break;
      default: fieldnames.add("Data");
              fieldvalues[i]=new ArrayList<>();
              fieldvalues[i].add("val10");
              break;
      }
    }
  }

  /**
   * Builds a value for a randomly chosen field.
   */
  private HashMap<String, ByteIterator> buildSingleValue(long keynum, String key) {
    HashMap<String, ByteIterator> value = new HashMap<>();

    int fieldnum = fieldchooser.nextValue().intValue();
    String fieldkey = fieldnames.get(fieldnum);
    ByteIterator data;
    if (dataintegrity) {
      data = new StringByteIterator(buildDeterministicValue(keynum, fieldnum, fieldkey));
    } else {
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    }
    value.put(fieldkey, data);

    return value;
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues(long keynum, String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (int i=0; i< fieldnames.size(); i++) {
      ByteIterator data;
      String fieldkey = fieldnames.get(i);
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(keynum, i, fieldkey));
      } else {
        // fill with random data
        data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      }
      values.put(fieldkey, data);
    }
    return values;
  }

  /**
   * Build a deterministic value given the key information.
   */
  private String buildDeterministicValue(long keynum, int fieldnum, String fieldkey) {
    int size = fieldlengthgenerator.nextValue().intValue();
    StringBuilder sb = new StringBuilder(size);
    //sb.append(fieldkey);
    //sb.append('=');
    if (fieldnum == 9) { //field10 is data; rest are metadata
      while (sb.length() < size) {
        sb.append(String.valueOf(keynum));
        sb.append(sb.toString().hashCode());
      }
      sb.setLength(size);
    } else {
      sb.append(fieldvalues[fieldnum].get((int)keynum%fieldvalues[fieldnum].size()));
    }
    return sb.toString();
  }

  private int buildTTLValue(long keynum) {
    // fieldvalue[1] = TTL
    return Integer.parseInt(fieldvalues[1].get((int)keynum%fieldvalues[1].size()));
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads,
   * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);
    int ttl = buildTTLValue(keynum);
    HashMap<String, ByteIterator> values = buildValues(keynum, dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insertTTL(table, dbkey, values, ttl);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    if (isFirst) {
      if (checkcompliance) {
        doTransactionCheckCompliance(db);
      } 
      if (readlog) {
        doTransactionReadLog(db);
      }
      isFirst = false;
    }

    switch (operation) {
    case "READMETAPURPOSE":
      doTransactionReadMeta(db, 0);
      break;
    case "READMETAUSER":
      doTransactionReadMeta(db, 2);
      break;
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATEMETAPURPOSE":
      doTransactionUpdateMeta(db, 0);
      break;
    case "UPDATEMETAUSER":
      doTransactionUpdateMeta(db, 2);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SCAN":
      doTransactionScan(db);
      break;
    case "DELETEMETAPURPOSE":
      doTransactionDeleteMeta(db, 0);
      break;
    case "DELETEMETAUSER":
      doTransactionDeleteMeta(db, 2);
      break;
    case "DELETE":
      doTransactionDelete(db);
      break;
    /*case "CHECKCOMPLIANCE":
      doTransactionCheckCompliance(db);
      break;*/
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }

  /**
   * Results are reported in the first three buckets of the histogram under
   * the label "VERIFY".
   * Bucket 0 means the expected data was returned.
   * Bucket 1 means incorrect data was returned.
   * Bucket 2 means null data was returned when some data was expected.
   */
  protected void verifyRow(long keynum, String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();
    int i = 0;
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString().equals(buildDeterministicValue(keynum, i++, entry.getKey()))) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      // This assumes that null data is never valid
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    measurements.reportStatus("VERIFY", verifyStatus);
  }

  long nextKeynum() {
    long keynum;
    if (keychooser instanceof ExponentialGenerator) {
      do {
        keynum = transactioninsertkeysequence.lastValue() - keychooser.nextValue().intValue();
      } while (keynum < 0);
    } else {
      do {
        keynum = keychooser.nextValue().intValue();
      } while (keynum > transactioninsertkeysequence.lastValue());
    }
    return keynum;
  }

  public void doTransactionRead(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    //System.err.println("Transaction read got called!");
    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);

    /*if (dataintegrity) {
      verifyRow(keynum, keyname, cells);
    }*/
  }

  public void doTransactionReadMeta(DB db, int metadatanum) {

    long keynum = nextKeynum();

    // match on meta data field passed
    String metadatacond = buildDeterministicValue(keynum, metadatanum, fieldnames.get(metadatanum));

    //System.err.println("Read metadata called with cond: "+ metadatacond + " Field num: " + metadatanum);

    db.readMeta(table, metadatanum, metadatacond, "key*", new Vector<HashMap<String, ByteIterator>>());
  }

  public void doTransactionReadLog(DB db) {
    // choose a random scan length
    int len = scanlength.nextValue().intValue();

    System.err.println("Read log called with scan len: "+ len);

    db.readLog(table, len);
  }

  public void doTransactionCheckCompliance(DB db) {

    long count = (long) (recordcount * 0.9);

    System.err.println("Verify conformance called with recordcount "+ count);

    db.verifyTTL(table, count);
  }

  public void doTransactionReadModifyWrite(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keynum, keyname);
    } else {
      // update a random field
      values = buildSingleValue(keynum, keyname);
    }

    // do the transaction

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();


    long ist = measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    db.read(table, keyname, fields, cells);

    db.update(table, keyname, values);

    long en = System.nanoTime();

    if (dataintegrity) {
      verifyRow(keynum, keyname, cells);
    }

    measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
  }

  public void doTransactionScan(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String startkeyname = buildKeyName(keynum);

    // choose a random scan length
    int len = scanlength.nextValue().intValue();

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
  }

  public void doTransactionUpdateMeta(DB db, int metadatanum) {

    //String startkeyname = buildKeyName(0);

    long keynum = nextKeynum();

    // match on metadata field
    String metadatacond = buildDeterministicValue(keynum, metadatanum, fieldnames.get(metadatanum));

    // pick another field to be updated
    int fieldnum = metadatachooser.nextValue().intValue();
    String fieldkey = fieldnames.get(fieldnum);

    // new value for another meta data field
    String metadatavalue = buildDeterministicValue(keynum, fieldnum, fieldkey);

    //System.err.println("Update metadata called with cond: "+ metadatacond +
    //                   " value: " + metadatavalue + " metadatanum " + metadatanum);
    
    db.updateMeta(table, metadatanum, metadatacond, "key*", fieldkey, metadatavalue);
  }

  public void doTransactionUpdate(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keynum, keyname);
    } else {
      // update a random field
      values = buildSingleValue(keynum, keyname);
    }

    db.update(table, keyname, values);
  }

  public void doTransactionDelete(DB db) {
    // choose a random key
    long keynum = nextKeynum();
    
    String keyname = buildKeyName(keynum);
    
    //System.err.println("Transaction delete called for: "+ keyname);
    
    db.delete(table, keyname);
  }

  public void doTransactionDeleteMeta(DB db, int metadatanum) {
    // choose a random key
    long keynum = nextKeynum();

    // match on metadata field
    String metadatacond = buildDeterministicValue(keynum, metadatanum, fieldnames.get(metadatanum));
    
    //System.err.println("Transaction delete meta called for: "+ metadatacond + " metadatanum: " + metadatanum);
    
    db.deleteMeta(table, metadatanum, metadatacond, "key*");
  }

  public void doTransactionInsert(DB db) {
    // choose the next key
    long keynum = transactioninsertkeysequence.nextValue();

    try {
      String dbkey = buildKeyName(keynum);

      int ttl = buildTTLValue(keynum);
      HashMap<String, ByteIterator> values = buildValues(keynum, dbkey);
      db.insertTTL(table, dbkey, values, ttl);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }

  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "READMETA", "UPDATE", "UPDATEMETA", 
   *  "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double readmetapurproportion = Double.parseDouble(
        p.getProperty(READMETA_PURPOSE_PROPORTION_PROPERTY, READMETA_PURPOSE_PROPORTION_PROPERTY_DEFAULT));
    final double readmetauserproportion = Double.parseDouble(
        p.getProperty(READMETA_USER_PROPORTION_PROPERTY, READMETA_USER_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double updatemetapurproportion = Double.parseDouble(
        p.getProperty(UPDATEMETA_PURPOSE_PROPORTION_PROPERTY, UPDATEMETA_PURPOSE_PROPORTION_PROPERTY_DEFAULT));
    final double updatemetauserproportion = Double.parseDouble(
        p.getProperty(UPDATEMETA_USER_PROPORTION_PROPERTY, UPDATEMETA_USER_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double deleteproportion = Double.parseDouble(
        p.getProperty(DELETE_PROPORTION_PROPERTY, DELETE_PROPORTION_PROPERTY_DEFAULT));
    final double deletemetapurproportion = Double.parseDouble(
        p.getProperty(DELETEMETA_PURPOSE_PROPORTION_PROPERTY, DELETEMETA_PURPOSE_PROPORTION_PROPERTY_DEFAULT));
    final double deletemetauserproportion = Double.parseDouble(
        p.getProperty(DELETEMETA_USER_PROPORTION_PROPERTY, DELETEMETA_USER_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (readmetapurproportion > 0) {
      operationchooser.addValue(readmetapurproportion, "READMETAPURPOSE");
    }

    if (readmetauserproportion > 0) {
      operationchooser.addValue(readmetauserproportion, "READMETAUSER");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    if (updatemetapurproportion > 0) {
      operationchooser.addValue(updatemetapurproportion, "UPDATEMETAPURPOSE");
    }

    if (updatemetauserproportion > 0) {
      operationchooser.addValue(updatemetauserproportion, "UPDATEMETAUSER");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (deleteproportion > 0) {
      operationchooser.addValue(deleteproportion, "DELETE");
    }

    if (deletemetapurproportion > 0) {
      operationchooser.addValue(deletemetapurproportion, "DELETEMETAPURPOSE");
    }

    if (deletemetauserproportion > 0) {
      operationchooser.addValue(deletemetauserproportion, "DELETEMETAUSER");
    }

    if (scanproportion > 0) {
      operationchooser.addValue(scanproportion, "SCAN");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }

    return operationchooser;
  }
}
