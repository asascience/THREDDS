/*
 * Copyright (c) 1998 - 2011. University Corporation for Atmospheric Research/Unidata
 * Portions of this software were developed by the Unidata Program at the
 * University Corporation for Atmospheric Research.
 *
 * Access and use of this software shall impose the following obligations
 * and understandings on the user. The user is granted the right, without
 * any fee or cost, to use, copy, modify, alter, enhance and distribute
 * this software, and any derivative works thereof, and its supporting
 * documentation for any purpose whatsoever, provided that this entire
 * notice appears in all copies of the software, derivative works and
 * supporting documentation.  Further, UCAR requests that the user credit
 * UCAR/Unidata in any publications that result from the use of this
 * software or in any product that includes this software. The names UCAR
 * and/or Unidata, however, may not be used in any advertising or publicity
 * to endorse or promote any products or commercial entity unless specific
 * written permission is obtained from UCAR/Unidata. The user also
 * understands that UCAR/Unidata is not obligated to provide the user with
 * any support, consulting, training or assistance of any kind with regard
 * to the use, operation and performance of this software nor to provide
 * the user with any updates, revisions, new versions or "bug fixes."
 *
 * THIS SOFTWARE IS PROVIDED BY UCAR/UNIDATA "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL UCAR/UNIDATA BE LIABLE FOR ANY SPECIAL,
 * INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING
 * FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION
 * WITH THE ACCESS, USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package ucar.nc2.grib.grib1.builder;

import com.google.protobuf.ByteString;
import thredds.featurecollection.FeatureCollectionConfig;
import thredds.inventory.*;
import thredds.inventory.MCollection;
import ucar.nc2.constants.CDM;
import ucar.nc2.grib.*;
import ucar.nc2.grib.grib1.*;
import ucar.nc2.grib.grib1.tables.Grib1Customizer;
import ucar.nc2.stream.NcStream;
import ucar.nc2.util.CloseableIterator;
import ucar.unidata.io.RandomAccessFile;
import ucar.unidata.util.Parameter;

import java.io.*;
import java.util.*;

/**
 * Build a GribCollection object for Grib-1 files. Manage grib collection index (ncx).
 * Covers GribCollectionProto, which serializes and deserializes.
 * Rectilyse means to turn the collection into a multidimensional variable.
 *
 * @author caron
 * @since 4/6/11
 */
public class Grib1CollectionBuilder extends GribCollectionBuilder {
  protected static final int minVersionSingle = 11; // if single file, this version and above is ok
  protected static final int version = 11;
  public static final String MAGIC_START = "Grib1CollectionIndex";

  // from a single file, read in the index, create if it doesnt exist or is out of date
  static public GribCollection readOrCreateIndexFromSingleFile(MFile file, CollectionUpdateType force,
                                                               FeatureCollectionConfig.GribConfig config, org.slf4j.Logger logger) throws IOException {
    Grib1CollectionBuilder builder = new Grib1CollectionBuilder(file, config, logger);
    builder.readOrCreateIndex(force);
    return builder.gc;
  }

  // called by tdm
  static public boolean update(CollectionManager dcm, org.slf4j.Logger logger) throws IOException {
    Grib1CollectionBuilder builder = new Grib1CollectionBuilder(dcm, logger);
    if (!builder.needsUpdate()) return false;
    builder.readOrCreateIndex(CollectionUpdateType.always);
    builder.gc.close();
    return true;
  }

  // from a collection, read in the index, create if it doesnt exist or is out of date
  // assume that the CollectionManager is up to date, eg doesnt need to be scanned
  static public GribCollection factory(MCollection dcm, CollectionUpdateType force, org.slf4j.Logger logger) throws IOException {
    Grib1CollectionBuilder builder = new Grib1CollectionBuilder(dcm, logger);
    builder.readOrCreateIndex(force);
    return builder.gc;
  }

  /* read in the index, index raf already open
  static public GribCollection createFromIndex(String name, File directory, RandomAccessFile indexRaf,
                                               FeatureCollectionConfig.GribConfig config, org.slf4j.Logger logger) throws IOException {
    Grib1CollectionBuilder builder = new Grib1CollectionBuilder(name, directory, config, logger);
    if (builder.readIndex(indexRaf)) {
      return builder.gc;
    }
    throw new IOException("Reading index failed");
  } */

  // this writes the index always
  static public boolean writeIndexFile(File indexFile, CollectionManager dcm, org.slf4j.Logger logger) throws IOException {
    Grib1CollectionBuilder builder = new Grib1CollectionBuilder(dcm, logger);
    return builder.createIndex(indexFile);
  }

  ////////////////////////////////////////////////////////////////

  //protected final List<CollectionManager> collections = new ArrayList<CollectionManager>();
  protected GribCollection gc;
  protected Grib1Customizer cust;
  protected String name;
  protected File directory;

  // single file
  private Grib1CollectionBuilder(MFile file, FeatureCollectionConfig.GribConfig config, org.slf4j.Logger logger) throws IOException {
    super(new CollectionSingleFile(file, logger), true, logger);
    this.name = file.getName();
    this.directory = new File(dcm.getRoot());

    try {
      if (config != null) dcm.putAuxInfo(FeatureCollectionConfig.AUX_GRIB_CONFIG, config);
      this.gc = new Grib1Collection(file.getName(), new File(dcm.getRoot()), config);

    } catch (Exception e) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream(10000);
      e.printStackTrace(new PrintStream(bos));
      logger.error("Failed to create index for single file", e);
      throw new IOException(e);
    }
  }

  private Grib1CollectionBuilder(MCollection dcm, org.slf4j.Logger logger) {
    super(dcm, false, logger);
    this.name = dcm.getCollectionName();
    this.directory = new File(dcm.getRoot());

    FeatureCollectionConfig.GribConfig config = (FeatureCollectionConfig.GribConfig) dcm.getAuxInfo(FeatureCollectionConfig.AUX_GRIB_CONFIG);
    this.gc = new Grib1Collection(dcm.getCollectionName(), new File(dcm.getRoot()), config);
  }

  protected Grib1CollectionBuilder(MCollection dcm, boolean isSingleFile, org.slf4j.Logger logger) {
    super(dcm, isSingleFile, logger);
  }


  // read or create index
  private void readOrCreateIndex(CollectionUpdateType ff) throws IOException {

    // force new index or test for new index needed
    boolean force = ((ff == CollectionUpdateType.always) || (ff == CollectionUpdateType.test && needsUpdate()));

    // otherwise, we're good as long as the index file exists
    File idx = gc.getIndexFile(); // LOOK problem - index exists but its out of date - trigger rewrite, but not writeable.
    if (force || !idx.exists() || !readIndex(idx.getPath()) )  {
      // write out index
      idx = gc.makeNewIndexFile(logger); // make sure we have a writeable index
      logger.info("{}: createIndex {}", gc.getName(), idx.getPath());
      createIndex(idx);

      // read back in index
      RandomAccessFile indexRaf = new RandomAccessFile(idx.getPath(), "r");
      gc.setIndexRaf(indexRaf);
      readIndex(indexRaf);
    }
  }

  public boolean needsUpdate() throws IOException {
    if (dcm == null) return false;
    File idx = gc.getIndexFile();
    return !idx.exists() || needsUpdate(idx.lastModified());
  }

  private boolean needsUpdate(long idxLastModified) throws IOException {
    CollectionManager.ChangeChecker cc = GribIndex.getChangeChecker();
    try (CloseableIterator<MFile> iter = dcm.getFileIterator()) {
     while (iter.hasNext()) {
       if (cc.hasChangedSince(iter.next(), idxLastModified)) return true;
     }
   }
    return false;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////
  // reading

  private boolean readIndex(String filename) throws IOException {
    return readIndex( new RandomAccessFile(filename, "r") );
  }

  private boolean readIndex(RandomAccessFile indexRaf) throws IOException {
    FeatureCollectionConfig.GribConfig config = (FeatureCollectionConfig.GribConfig) dcm.getAuxInfo(FeatureCollectionConfig.AUX_GRIB_CONFIG);
    try {
      gc = Grib1CollectionBuilderFromIndex.createFromIndex(this.name, this.directory, indexRaf, config, logger);
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////
  // writing

  private class Group {
    public Grib1SectionGridDefinition gdss;
    public int gdsHash; // may have been modified
    public Grib1Rectilyser rect;
    public List<Grib1Record> records = new ArrayList<Grib1Record>();
    public String nameOverride;
    public Set<Integer> fileSet; // this is so we can show just the component files that are in this group

    private Group(Grib1SectionGridDefinition gdss, int gdsHash) {
      this.gdss = gdss;
      this.gdsHash = gdsHash;
    }
  }

  ///////////////////////////////////////////////////
  // create the index

  private boolean createIndex(File indexFile) throws IOException {
    if (dcm == null) {
      logger.error("Grib1CollectionBuilder "+gc.getName()+" : cannot create new index ");
      throw new IllegalStateException();
    }
    long start = System.currentTimeMillis();

    ArrayList<MFile> files = new ArrayList<MFile>();
    List<Group> groups = makeAggregatedGroups(files);
    createIndex(indexFile, groups, files);

    long took = System.currentTimeMillis() - start;
    if (logger.isDebugEnabled())
      logger.debug("That took {} msecs", took);
    return true;
  }

  // read all records in all files,
  // divide into groups based on GDS hash
  // each group has an arraylist of all records that belong to it.
  // for each group, run rectlizer to derive the coordinates and variables
  public List<Group> makeAggregatedGroups(ArrayList<MFile> files) throws IOException {
    Map<Integer, Group> gdsMap = new HashMap<Integer, Group>();
    Map<Integer, Integer> gdsConvert = null;
    Map<String, Boolean> pdsConvert = null;
    Grib1Rectilyser.Counter stats = new Grib1Rectilyser.Counter();
    //boolean intvMerge = intvMergeDefault;

    logger.debug("GribCollection {}: makeAggregatedGroups%n", gc.getName());
    int fileno = 0;
    logger.debug(" dcm= {}%n", dcm);

    FeatureCollectionConfig.GribConfig config = (FeatureCollectionConfig.GribConfig) dcm.getAuxInfo(FeatureCollectionConfig.AUX_GRIB_CONFIG);
    if (config != null) gdsConvert = config.gdsHash;
    if (config != null) pdsConvert = config.pdsHash;
    FeatureCollectionConfig.GribIntvFilter intvMap = (config != null) ?  config.intvFilter : null;
    // intvMerge = (config == null) || (config.intvMerge == null) ? intvMergeDefault : config.intvMerge;

    for (MFile mfile : dcm.getFilesSorted()) {  // LOOK do we really need sorted ??

      Grib1Index index;
      try {
        index = (Grib1Index) GribIndex.readOrCreateIndexFromSingleFile(true, !isSingleFile, mfile, config, CollectionUpdateType.test, logger);
        files.add(mfile);  // only add on success

      } catch (IOException ioe) {
        logger.error("Grib1CollectionBuilder "+gc.getName()+" : reading/Creating gbx9 index for file "+ mfile.getPath()+" failed", ioe);
        continue;
      }

      for (Grib1Record gr : index.getRecords()) {
        gr.setFile(fileno); // each record tracks which file it belongs to
        int gdsHash = gr.getGDSsection().getGDS().hashCode();      // use GDS hash code to group records
        if (gdsConvert != null && gdsConvert.get(gdsHash) != null) // allow external config to muck with gdsHash. Why? because of error in encoding
          gdsHash = gdsConvert.get(gdsHash);                       // and we need exact hash matching
        if (cust == null)
          cust = Grib1Customizer.factory(gr, null);
        if (config != null)
          cust.setTimeUnitConverter(config.getTimeUnitConverter());
        if (intvMap != null && filterOut(gr, intvMap)) {
          stats.filter++;
          continue; // skip
        }

        Group g = gdsMap.get(gdsHash);
        if (g == null) {
          g = new Group(gr.getGDSsection(), gdsHash);
          gdsMap.put(gdsHash, g);
          //g.nameOverride = setGroupNameOverride(gdsHash, gdsNamer, groupNamer, mfile);
        }
        g.records.add(gr);
      }
      fileno++;
      stats.recordsTotal += index.getRecords().size();
    }

    List<Group> result = new ArrayList<Group>(gdsMap.values());
    for (Group g : result) {
      g.rect = new Grib1Rectilyser(cust, g.records, g.gdsHash, pdsConvert);
      g.rect.make(stats);
    }

    if (logger.isDebugEnabled()) logger.debug(stats.show());
    return result;
  }

    // true means remove
  private boolean filterOut(Grib1Record gr, FeatureCollectionConfig.GribIntvFilter intvFilter) {
    Grib1SectionProductDefinition pdss = gr.getPDSsection();
    Grib1ParamTime ptime = pdss.getParamTime(cust);
    if (!ptime.isInterval()) return false;

    int[] intv = ptime.getInterval();
    if (intv == null) return false;
    int haveLength = intv[1] - intv[0];

    // HACK
    if (haveLength == 0 && intvFilter.isZeroExcluded()) {  // discard 0,0
      if ((intv[0] == 0) && (intv[1] == 0)) {
        //f.format(" FILTER INTV [0, 0] %s%n", gr);
        return true;
      }
      return false;

    } else if (intvFilter.hasFilter()) {
      int center = pdss.getCenter();
      int subcenter = pdss.getSubCenter();
      int version = pdss.getTableVersion();
      int param = pdss.getParameterNumber();
      int id = (center << 8) + (subcenter << 16) + (version << 24) + param;

      return !intvFilter.filterOk(id, haveLength, Integer.MIN_VALUE);
    }
    return false;
  }

  ///////////////////////////////////////////////////////////////////////////////////

  public String getMagicStart() {
    return MAGIC_START;
  }

  /*
   MAGIC_START
   version
   sizeRecords
   VariableRecords (sizeRecords bytes)
   sizeIndex
   GribCollectionIndex (sizeIndex bytes)
   */

  private void createIndex(File indexFile, List<Group> groups, ArrayList<MFile> files) throws IOException {
    Grib1Record first = null; // take global metadata from here
    boolean deleteOnClose = false;

    if (indexFile.exists()) {
      if (!indexFile.delete())
        logger.warn(" gc1 cant delete index file {}", indexFile.getPath());
    }
    logger.debug(" createIndex for {}", indexFile.getPath());

    RandomAccessFile raf = new RandomAccessFile(indexFile.getPath(), "rw");
    raf.order(RandomAccessFile.BIG_ENDIAN);
    try {
      //// header message
      raf.write(getMagicStart().getBytes(CDM.utf8Charset));
      raf.writeInt(version);
      long lenPos = raf.getFilePointer();
      raf.writeLong(0); // save space to write the length of the record section
      long countBytes = 0;
      int countRecords = 0;
      for (Group g : groups) {
        g.fileSet = new HashSet<Integer>();
        for (Grib1Rectilyser.VariableBag vb : g.rect.getGribvars()) {
          if (first == null) first = vb.first;
          GribCollectionProto.VariableRecords vr = writeRecordsProto(vb, g.fileSet);
          byte[] b = vr.toByteArray();
          vb.pos = raf.getFilePointer();
          vb.length = b.length;
          raf.write(b);
          countBytes += b.length;
          countRecords += vb.recordMap.length;
        }
      }
      long bytesPerRecord = countBytes / ((countRecords == 0) ? 1 : countRecords);
      if (logger.isDebugEnabled())
        logger.debug("  write RecordMaps: bytes = {} records = {} bytesPerRecord={}", countBytes, countRecords, bytesPerRecord);

      if (first == null) {
        deleteOnClose = true;
        logger.error("GribCollection {}: has no files", gc.getName());
        throw new IOException("GribCollection " + gc.getName() + " has no files");
      }

      long pos = raf.getFilePointer();
      raf.seek(lenPos);
      raf.writeLong(countBytes);
      raf.seek(pos); // back to the output.

      GribCollectionProto.GribCollectionIndex.Builder indexBuilder = GribCollectionProto.GribCollectionIndex.newBuilder();
      indexBuilder.setName(gc.getName());

      // directory and mfile list
      indexBuilder.setDirName(gc.getDirectory().getPath());
      List<GribCollectionBuilder.GcMFile> gcmfiles = GribCollectionBuilder.makeFiles(gc.getDirectory(), files, null);
      for (GribCollectionBuilder.GcMFile gcmfile : gcmfiles) {
        indexBuilder.addMfiles(gcmfile.makeProto());
      }

      for (Group g : groups)
        indexBuilder.addGroups(writeGroupProto(g));

      /* int count = 0;
      for (DatasetCollectionManager dcm : collections) {
        indexBuilder.addParams(makeParamProto(new Parameter("spec" + count, dcm.())));
        count++;
      } */

      // what about just storing first ??
      Grib1SectionProductDefinition pds = first.getPDSsection();
      indexBuilder.setCenter(pds.getCenter());
      indexBuilder.setSubcenter(pds.getSubCenter());
      indexBuilder.setLocal(pds.getTableVersion());
      indexBuilder.setMaster(0);
      indexBuilder.setGenProcessId(pds.getGenProcess());

      GribCollectionProto.GribCollectionIndex index = indexBuilder.build();
      byte[] b = index.toByteArray();
      NcStream.writeVInt(raf, b.length); // message size
      raf.write(b);  // message  - all in one gulp
      logger.debug("  write GribCollectionIndex= {} bytes", b.length);

    } finally {
      logger.debug("  file size =  %d bytes", raf.length());
      raf.close();

      // remove it on failure
      if (deleteOnClose && !indexFile.delete())
        logger.error(" gc1 cant deleteOnClose index file {}", indexFile.getPath());
    }
  }

  private GribCollectionProto.VariableRecords writeRecordsProto(Grib1Rectilyser.VariableBag vb, Set<Integer> fileSet) throws IOException {
    GribCollectionProto.VariableRecords.Builder b = GribCollectionProto.VariableRecords.newBuilder();
    b.setCdmHash(vb.cdmHash);
    for (Grib1Rectilyser.Record ar : vb.recordMap) {
      GribCollectionProto.Record.Builder br = GribCollectionProto.Record.newBuilder();

      if (ar == null || ar.gr == null) {
        br.setFileno(0);
        br.setPos(0);
        br.setMissing(true); // missing : cant use 0 since that may be a valid value

      } else {
        br.setFileno(ar.gr.getFile());
        fileSet.add(ar.gr.getFile());
        Grib1SectionIndicator is = ar.gr.getIs();
        br.setPos(is.getStartPos()); // start of entire message
      }
      b.addRecords(br);
    }
    return b.build();
  }

  private GribCollectionProto.Group writeGroupProto(Group g) throws IOException {
    GribCollectionProto.Group.Builder b = GribCollectionProto.Group.newBuilder();

    if (g.gdss.getPredefinedGridDefinition() >= 0)
      b.setPredefinedGds(g.gdss.getPredefinedGridDefinition());
    else {
      b.setGds(ByteString.copyFrom(g.gdss.getRawBytes()));
      b.setGdsHash(g.gdsHash);
  }

    for (Grib1Rectilyser.VariableBag vb : g.rect.getGribvars())
      b.addVariables(writeVariableProto(g.rect, vb));

    List<TimeCoord> timeCoords = g.rect.getTimeCoords();
    for (int i = 0; i < timeCoords.size(); i++)
      b.addTimeCoords(writeCoordProto(timeCoords.get(i), i));

    List<VertCoord> vertCoords = g.rect.getVertCoords();
    for (int i = 0; i < vertCoords.size(); i++)
      b.addVertCoords(writeCoordProto(vertCoords.get(i), i));

    List<EnsCoord> ensCoords = g.rect.getEnsCoords();
    for (int i = 0; i < ensCoords.size(); i++)
      b.addEnsCoords(writeCoordProto(ensCoords.get(i), i));

    for (Integer aFileSet : g.fileSet)
      b.addFileno(aFileSet);

    if (g.nameOverride != null)
      b.setName(g.nameOverride);

    return b.build();
  }

  private GribCollectionProto.Variable writeVariableProto(Grib1Rectilyser rect, Grib1Rectilyser.VariableBag vb) throws IOException {
    GribCollectionProto.Variable.Builder b = GribCollectionProto.Variable.newBuilder();
    Grib1SectionProductDefinition pds = vb.first.getPDSsection();

    b.setDiscipline(0);
    b.setCategory(0);
    b.setParameter(pds.getParameterNumber());
    b.setTableVersion(pds.getTableVersion()); // can differ for variables in the same file
    b.setLevelType(pds.getLevelType());
    b.setIsLayer(cust.isLayer(pds.getLevelType())); // LOOK alternatively could store an entire PDS (one for each variable)
    b.setCdmHash(vb.cdmHash);
    b.setRecordsPos(vb.pos);
    b.setRecordsLen(vb.length);
    b.setTimeIdx(vb.timeCoordIndex);
    if (vb.vertCoordIndex >= 0)
      b.setVertIdx(vb.vertCoordIndex);
    if (vb.ensCoordIndex >= 0)
      b.setEnsIdx(vb.ensCoordIndex);

    Grib1ParamTime ptime = pds.getParamTime(cust); // LOOK could use  cust.getParamTime(pds) to not retain object
    if (ptime.isInterval()) {
      b.setIntervalType(pds.getTimeRangeIndicator());
      b.setIntvName(rect.getTimeIntervalName(vb.timeCoordIndex));
    }

    /* if (pds.isEnsembleDerived()) {
      Grib1Pds.PdsEnsembleDerived pdsDerived = (Grib1Pds.PdsEnsembleDerived) pds;
      b.setEnsDerivedType(pdsDerived.getDerivedForecastType()); // derived type (table 4.7)
    }

    if (pds.isProbability()) {
      Grib1Pds.PdsProbability pdsProb = (Grib1Pds.PdsProbability) pds;
      b.setProbabilityName(pdsProb.getProbabilityName());
      b.setProbabilityType(pdsProb.getProbabilityType());
    } */

    return b.build();
  }

  protected GribCollectionProto.Parameter writeParamProto(Parameter param) throws IOException {
    GribCollectionProto.Parameter.Builder b = GribCollectionProto.Parameter.newBuilder();

    b.setName(param.getName());
    if (param.isString())
      b.setSdata(param.getStringValue());
    else {
      for (int i = 0; i < param.getLength(); i++)
        b.addData(param.getNumericValue(i));
    }

    return b.build();
  }

  protected GribCollectionProto.Coord writeCoordProto(TimeCoord tc, int index) throws IOException {
    GribCollectionProto.Coord.Builder b = GribCollectionProto.Coord.newBuilder();
    b.setIndex(index);
    b.setCode(tc.getCode());
    b.setUnit(tc.getUnits());
    float scale = (float) tc.getTimeUnitScale(); // deal with, eg, "6 hours" by multiplying values by 6
    if (tc.isInterval()) {
      for (TimeCoord.Tinv tinv : tc.getIntervals()) {
        b.addValues(tinv.getBounds1() * scale);
        b.addBound(tinv.getBounds2() * scale);
      }
    } else {
      for (int value : tc.getCoords())
        b.addValues(value * scale);
    }
    return b.build();
  }

  protected GribCollectionProto.Coord writeCoordProto(VertCoord vc, int index) throws IOException {
    GribCollectionProto.Coord.Builder b = GribCollectionProto.Coord.newBuilder();
    b.setIndex(index);
    b.setCode(vc.getCode());
    String units = (vc.getUnits() != null) ? vc.getUnits() : "";
    b.setUnit(units);
    for (VertCoord.Level coord : vc.getCoords()) {
      if (vc.isLayer()) {
        b.addValues((float) coord.getValue1());
        b.addBound((float) coord.getValue2());
      } else {
        b.addValues((float) coord.getValue1());
      }
    }
    return b.build();
  }

  protected GribCollectionProto.Coord writeCoordProto(EnsCoord ec, int index) throws IOException {
    GribCollectionProto.Coord.Builder b = GribCollectionProto.Coord.newBuilder();
    b.setIndex(index);
    b.setCode(0);
    b.setUnit("");
    for (EnsCoord.Coord coord : ec.getCoords()) {
      b.addValues((float) coord.getCode());
      b.addValues((float) coord.getEnsMember());
    }
    return b.build();
  }

}