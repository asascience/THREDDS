package ucar.nc2.grib.collection;

import com.google.protobuf.ByteString;
import thredds.featurecollection.FeatureCollectionConfig;
import thredds.inventory.CollectionUpdateType;
import thredds.inventory.MCollection;
import thredds.inventory.partition.PartitionManager;
import ucar.ma2.Section;
import ucar.sparr.*;
import ucar.nc2.constants.CDM;
import ucar.nc2.stream.NcStream;
import ucar.unidata.io.RandomAccessFile;
import ucar.unidata.util.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Builds Grib2 PartitionCollections (version 2)
 *
 * @author John
 * @since 12/7/13
 */
public class Grib2PartitionBuilder extends Grib2CollectionWriter {
  public static final String MAGIC_START = "Grib2Partition2Index";  // was Grib2Partition0Index
  static private final boolean trace = false;

  // read in the index, create if it doesnt exist or is out of date (depends on force)
  static public Grib2Partition factory(PartitionManager tpc, CollectionUpdateType forcePartition, CollectionUpdateType forceChildren,
                                       Formatter errlog, org.slf4j.Logger logger) throws IOException {
    Grib2PartitionBuilder builder = new Grib2PartitionBuilder(tpc.getCollectionName(), new File(tpc.getRoot()), tpc, logger);
    builder.readOrCreateIndex(forcePartition, forceChildren, errlog);
    return builder.result;
  }

  // recreate if partition doesnt exist or is out of date (depends on force). return true if it was recreated
  static public boolean recreateIfNeeded(PartitionManager tpc, CollectionUpdateType forcePartition, CollectionUpdateType forceChildren,
                                         Formatter errlog, org.slf4j.Logger logger) throws IOException {
    Grib2PartitionBuilder builder = new Grib2PartitionBuilder(tpc.getCollectionName(), new File(tpc.getRoot()), tpc, logger);
    boolean recreated = builder.readOrCreateIndex(forcePartition, forceChildren, errlog);
    builder.result.close();
    return recreated;
  }

  /* make the index
  static public boolean makePartitionIndex(PartitionManager tpc, Formatter errlog, org.slf4j.Logger logger) throws IOException {
    Grib2PartitionBuilder builder = new Grib2PartitionBuilder(tpc.getCollectionName(), new File(tpc.getRoot()), tpc, logger);
    builder.result.close();
    return builder.createPartitionedIndex(errlog);
  } */

  //////////////////////////////////////////////////////////////////////////////////

  private final PartitionManager partitionManager; // defines the partition
  private Grib2Partition result;  // build this object
  private String name;            // collection name
  private File directory;         // top directory

  protected Grib2PartitionBuilder(String name, File directory, PartitionManager tpc, org.slf4j.Logger logger) {
    super(tpc, logger);
    this.name = name;
    this.directory = directory;

    FeatureCollectionConfig.GribConfig config = null;
    if (tpc != null)
      config = (FeatureCollectionConfig.GribConfig) tpc.getAuxInfo(FeatureCollectionConfig.AUX_GRIB_CONFIG);
    this.result = new Grib2Partition(name, directory, config, logger);
    this.partitionManager = tpc;
  }

  // return true if the partition was recreated
  private boolean readOrCreateIndex(CollectionUpdateType forcePartition, CollectionUpdateType forceChildren, Formatter errlog) throws IOException {
    File idx = result.getIndexFile();

    // force new index or test for new index needed
    boolean force = ((forcePartition == CollectionUpdateType.always) || (forcePartition == CollectionUpdateType.test && needsUpdate(idx.lastModified())));

    // otherwise, we're good as long as the index file exists and can be read
    if (force || !idx.exists() || !readIndex(idx.getPath())) {
      if (forcePartition == CollectionUpdateType.never) throw new IOException("failed to read " + idx.getPath());

      logger.info("{}: createIndex {}", name, idx.getPath());
      return createPartitionedIndex(forcePartition, forceChildren, errlog);
    }
    return false;
  }

  private boolean needsUpdate(long collectionLastModified) throws IOException {
    for (MCollection dcm : partitionManager.makePartitions(CollectionUpdateType.test)) {
      File idxFile = GribCollection.getIndexFile(dcm.getIndexFilename());
      if (!idxFile.exists())
        return true;
      if (collectionLastModified < idxFile.lastModified())
        return true;
    }
    return false;
  }

  private boolean readIndex(String filename) throws IOException {
    return readIndex(new RandomAccessFile(filename, "r"));
  }

  private boolean readIndex(RandomAccessFile indexRaf) throws IOException {
    try {
      this.result = (Grib2Partition) Grib2PartitionBuilderFromIndex.createTimePartitionFromIndex(this.name, this.directory, indexRaf, result.getGribConfig(), logger);
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }

  ///////////////////////////////////////////////////
  // create the index

  private boolean createPartitionedIndex(CollectionUpdateType forcePartition, CollectionUpdateType forceChildren, Formatter errlog) throws IOException {
    long start = System.currentTimeMillis();
    if (errlog == null) errlog = new Formatter(); // info will be discarded

    try {
      // create partitions
      for (MCollection dcm : partitionManager.makePartitions(forcePartition)) {
        result.addPartition(dcm);
      }

      List<PartitionCollection.Partition> bad = new ArrayList<>();
      for (PartitionCollection.Partition tpp : result.getPartitions()) {
        try {
          tpp.gc = tpp.makeGribCollection(forceChildren);  // here we read in all collections at once. can we avoid this?
          if (trace) logger.debug(" Open partition {}", tpp.getDcm().getCollectionName());

        } catch (Throwable t) {
          logger.error(" Failed to open partition " + tpp.getName(), t);
          bad.add(tpp);
        }
      }

      // remove ones that failed
      for (PartitionCollection.Partition tpp : bad)
        result.removePartition(tpp);
      result.sortPartitions(); // after this cannot change

      // choose the "canonical" partition, aka prototype
      // only used in copyInfo
      int n = result.getPartitionSize();
      if (n == 0) {
        errlog.format("ERR Nothing in this partition = %s%n", result.getName());
        logger.error(" Nothing in this partition = {}", result.getName());
        return false;
      }
      int idx = partitionManager.getProtoIndex(n);
      PartitionCollection.Partition canon = result.getPartition(idx);
      logger.debug("     Using canonical partition {}", canon.getDcm().getCollectionName());

      // copy info from canon gribCollection to result partitionCollection
      result.copyInfo(canon.gc);
      result.isPartitionOfPartitions = (canon.gc instanceof Grib2Partition);

      // check consistency across vert and ens coords
      // create partitioned variables
      // partition index is used - do not resort partitions
      PartitionCollection.Dataset ds2D = makeDataset2D(errlog);
      if (ds2D == null) {
        errlog.format(" ERR makeDataset2D failed, index not written on %s%n", result.getName());
        logger.error(" makeDataset2D failed, index not written on {} errors = \n{}", result.getName(), errlog.toString());
        return false;
      }

      // this finishes the 2D stuff
      result.makeHorizCS();

      makeDatasetBest(ds2D, errlog);

      // ready to write the index file
      writeIndex(result, errlog);

    } finally {
      // close open gc's
      for (PartitionCollection.Partition tpp : result.getPartitions()) {
        tpp.gc.close();
      }
    }

    long took = System.currentTimeMillis() - start;
    errlog.format(" INFO CreatePartitionedIndex took %d msecs%n", took);
    return true;
  }

  private class GroupPartitions {
    GribCollection.GroupGC resultGroup;
    GribCollection.GroupGC[] componentGroups; // one for each partition; may be null if group is not in the partition
    int[] componentGroupIndex;                 // one for each partition; the index into the partition.ds2d.groups() array
    int npart;

    GroupPartitions(GribCollection.GroupGC resultGroup, int npart) {
      this.resultGroup = resultGroup;
      this.npart = npart;
      this.componentGroups = new GribCollection.GroupGC[npart];
      this.componentGroupIndex = new int[npart];
    }

    void makeVariableIndexPartitioned() {
      // find unique variables across all partitions
      Map<Integer, GribCollection.VariableIndex> varMap = new HashMap<>(2 * resultGroup.variList.size());
      for (GribCollection.GroupGC group : componentGroups) {
        if (group == null) continue;
        for (GribCollection.VariableIndex vi : group.variList)
          varMap.put(vi.cdmHash, vi); // this will use the last one found
      }
      for (GribCollection.VariableIndex vi : varMap.values()) {
        // convert each VariableIndex to VariableIndexPartitioned in result. note not using canon
        result.makeVariableIndexPartitioned(resultGroup, vi, npart); // this adds to resultGroup
      }
    }
  }

  // a list of unique groups across all partitions
  // as well as component groups for each group
  private List<GroupPartitions> makeGroupPartitions(PartitionCollection.Dataset ds2D, int npart) throws IOException {
    Map<Integer, GroupPartitions> groupMap = new HashMap<>(40);  // gdsHash, GroupPartition
    int countPartition = 0;
    for (PartitionCollection.Partition tpp : result.getPartitions()) {
      GribCollection gc = tpp.gc;
      GribCollection.Dataset ds2dp = gc.getDatasetCanonical(); // the twoD or GC dataset

      int groupIdx = 0;
      for (GribCollection.GroupGC g : ds2dp.groups) { // for each group in the partition
        GroupPartitions gs = groupMap.get(g.getGdsHash());
        if (gs == null) {
          gs = new GroupPartitions(ds2D.addGroupCopy(g), npart);
          groupMap.put(g.getGdsHash(), gs);
        }
        gs.componentGroups[countPartition] = g;
        gs.componentGroupIndex[countPartition] = groupIdx++;
      }
      countPartition++;
    }
    return new ArrayList<>(groupMap.values());
  }

  private PartitionCollection.Dataset makeDataset2D(Formatter f) throws IOException {
    FeatureCollectionConfig.GribConfig config = (FeatureCollectionConfig.GribConfig) dcm.getAuxInfo(FeatureCollectionConfig.AUX_GRIB_CONFIG);
    FeatureCollectionConfig.GribIntvFilter intvMap = (config != null) ? config.intvFilter : null;

    PartitionCollection.Dataset ds2D = result.makeDataset(GribCollection.Type.TwoD);

    int npart = result.getPartitionSize();
    List<GroupPartitions> groupPartitions = makeGroupPartitions(ds2D, npart);

        // make a complete set of runtimes
    CoordinateBuilder runtimeAllBuilder = new CoordinateRuntime.Builder();
    for (PartitionCollection.Partition tpp : result.getPartitions()) {
      GribCollection gc = tpp.gc;
      CoordinateRuntime partRuntime = gc.getMasterRuntime();
      runtimeAllBuilder.addAll(partRuntime);
    }
    result.masterRuntime = (CoordinateRuntime) runtimeAllBuilder.finish();

    // for each run, which partition ??
    result.run2part = new int[result.masterRuntime.getSize()];
    int partIdx = 0;
    for (PartitionCollection.Partition tpp : result.getPartitions()) {
      GribCollection gc = tpp.gc;
      CoordinateRuntime partRuntime = gc.getMasterRuntime();
      for (Object val : partRuntime.getValues()) {
        int idx = result.masterRuntime.getIndex(val);
        result.run2part[idx] = partIdx;     // note that later partitions will override earlier if they have the same runtime
      }
      partIdx++;
    }

     // do each group
    for (GroupPartitions gp : groupPartitions) {
      GribCollection.GroupGC resultGroup = gp.resultGroup;
      gp.makeVariableIndexPartitioned();

      String gname = resultGroup.getId();
      String gdesc = resultGroup.getDescription();

      // for each partition
      for (int partno = 0; partno < npart; partno++) {
        GribCollection.GroupGC group = gp.componentGroups[partno];
        if (group == null) {  // missing group in this partition
          f.format(" INFO canonical group %s not in partition %s%n", gname, result.getPartition(partno).getName());
          continue;
        }
        int groupIdx = gp.componentGroupIndex[partno];

        // for each variable in this Partition, add reference to it in the vip
        for (int varIdx = 0; varIdx < group.variList.size(); varIdx++) {
          GribCollection.VariableIndex vi = group.variList.get(varIdx);
          int flag = 0;
          PartitionCollection.VariableIndexPartitioned vip = (PartitionCollection.VariableIndexPartitioned) resultGroup.findVariableByHash(vi.cdmHash);
          vip.addPartition(partno, groupIdx, varIdx, flag, vi);
        } // loop over variable
      } // loop over partition

      // each VariableIndexPartitioned now has its list of PartitionForVariable

      // overall set of unique coordinates
      boolean isDense = (config != null) && "dense".equals(config.getParameter("CoordSys"));  // LOOK for now, assume non-dense
      CoordinateSharer sharify = new CoordinateSharer(isDense);

      // for each variable, create union of coordinates across the partitions
      for (GribCollection.VariableIndex viResult : resultGroup.variList) {
        // loop over partitions, make union coordinate; also time filter the intervals
        CoordinateUnionizer unionizer = new CoordinateUnionizer(viResult.getVarid(), intvMap);
        for (int partno = 0; partno < npart; partno++) {
          GribCollection.GroupGC group = gp.componentGroups[partno];
          if (group == null) continue; // tolerate missing groups
          GribCollection.VariableIndex vi = group.findVariableByHash(viResult.cdmHash);
          if (vi == null) continue; // tolerate missing variables
          unionizer.addCoords(vi.getCoordinates());
        }  // loop over partition

        viResult.coords = unionizer.finish();
        sharify.addCoords(viResult.coords);
      } // loop over variable

      // create a list of common coordinates, put them into the group, and now variables just reference those by index
      sharify.finish();
      resultGroup.coords = sharify.getUnionCoords();

      // debug
      List<CoordinateTime2D> time2DCoords = new ArrayList<>();
      Map<CoordinateRuntime, CoordinateRuntime> runtimes = new HashMap<>();
      for (Coordinate coord : resultGroup.coords) {
        Coordinate.Type type = coord.getType();
        switch (type) {
          case runtime:
            CoordinateRuntime reftime = (CoordinateRuntime) coord;
            runtimes.put(reftime, reftime);
            break;

          case time2D:
            CoordinateTime2D t2d = (CoordinateTime2D) coord;
            time2DCoords.add(t2d);
            break;
        }
      }
      for (CoordinateTime2D t2d : time2DCoords) {
        CoordinateRuntime runtime2D = t2d.getRuntimeCoordinate();
        CoordinateRuntime runtime = runtimes.get(runtime2D);
        if (runtime == null)
          System.out.printf("HEY assignRuntimeNames failed on %s group %s%n", t2d.getName(), resultGroup.getId());
      } // end debug

      for (GribCollection.VariableIndex viResult : resultGroup.variList) {
        // redo the variables against the shared coordinates
        viResult.coordIndex = sharify.reindex2shared(viResult.coords);

        // figure out missing data for each variable in the twoD time array
        if (result.isPartitionOfPartitions) {
          viResult.twot = null;
        } else
          makeMissing(isDense, gp, result, viResult); // PofGC only
      }

    } // loop over groups

    return ds2D;
  }

  private void makeMissing(boolean isDense, GroupPartitions gp, PartitionCollection result, GribCollection.VariableIndex viResult) throws IOException {
    Coordinate cr = viResult.getCoordinate(Coordinate.Type.runtime);
    Coordinate ct = viResult.getCoordinate(Coordinate.Type.time);
    if (ct == null) ct = viResult.getCoordinate(Coordinate.Type.timeIntv);
    if (ct == null) ct = viResult.getCoordinate(Coordinate.Type.time2D);
    if (cr == null) {
      logger.error("Missing runtime coordinate vi="+viResult.toStringShort());
      return;
    }
    if (ct == null) {
      logger.error("Missing time coordinate vi="+viResult.toStringShort());
      return;
    }

    int ntimes = (ct instanceof CoordinateTime2D) ? ((CoordinateTime2D)ct).getNtimes() : ct.getSize();
    CoordinateTwoTimer twot = new CoordinateTwoTimer(cr.getSize(), ntimes);
    viResult.twot = twot;

    if (isDense) {
      Map<Object, Integer> ctMap = new HashMap<>(2*ct.getSize());  // time coord val -> index in ct
      for (int i=0; i<ct.getSize(); i++) ctMap.put(ct.getValue(i), i);

      // loop over runtimes
      int runIdx = 0;
      for (int partno : result.run2part) {  // We only do this for PofGC, so  partitions are GC and have only one runtime, so no duplicate counting
        // get the partition/group/variable for this run
        GribCollection.GroupGC group = gp.componentGroups[partno];
        if (group == null) continue; // tolerate missing groups
        GribCollection.VariableIndex vi = group.findVariableByHash(viResult.cdmHash);
        if (vi == null) continue; // tolerate missing variables

        Coordinate ctP = vi.getCoordinate(Coordinate.Type.time);
        if (ctP == null) ctP = vi.getCoordinate(Coordinate.Type.timeIntv);
        if (ctP == null) ctP = vi.getCoordinate(Coordinate.Type.time2D);

        // we need the sparse array for this component vi
        vi.readRecords();
        SparseArray<GribCollection.Record> sa = vi.getSparseArray();
        Section s = new Section(sa.getShape());
        Section.Iterator iter = s.getIterator(sa.getShape());

        // run through all the inventory in this component vi
        int[] index = new int[sa.getRank()];
        while (iter.hasNext()) {
          int linearIndex = iter.next(index);
          if (sa.getContent(linearIndex) == null) continue;

          // convert value in component vi to index in result
          int timeIdxP = index[1];
          Object val = ctP.getValue(timeIdxP);
          Integer timeIdxR = ctMap.get(val);
          if (timeIdxR != null) {
            twot.add(runIdx, timeIdxR);
          }
        }

        runIdx++;
      }


    } else {

      // loop over runtimes
      int runIdx = 0;
      for (int partno : result.run2part) {  // We only do this for PofGC, so  partitions are GC and have only one runtime, so no duplicate counting
        // get the partition/group/variable for this run
        GribCollection.GroupGC group = gp.componentGroups[partno];
        if (group == null) continue; // tolerate missing groups
        GribCollection.VariableIndex vi = group.findVariableByHash(viResult.cdmHash);
        if (vi == null) continue; // tolerate missing variables

        // we need the sparse array for this component vi
        vi.readRecords();
        SparseArray<GribCollection.Record> sa = vi.getSparseArray();
        Section s = new Section(sa.getShape());
        Section.Iterator iter = s.getIterator(sa.getShape());

        // run through all the inventory in this component vi
        int[] index = new int[sa.getRank()];
        while (iter.hasNext()) {
          int linearIndex = iter.next(index);
          if (sa.getContent(linearIndex) == null) continue;
          int timeIdx = index[1];
          twot.add(runIdx, timeIdx);
        }

        runIdx++;
      }
    }
  }

  private void makeDatasetBest(GribCollection.Dataset ds2D, Formatter f) throws IOException {
    GribCollection.Dataset dsa = result.makeDataset(GribCollection.Type.Best);

    int npart = result.getPartitionSize();

     // do each group
    for (GribCollection.GroupGC group2D : ds2D.groups) {
      GribCollection.GroupGC groupB = dsa.addGroupCopy(group2D);  // make copy of group, add to dataset
      groupB.isTwod = false;

      // runtime offsets
      CoordinateRuntime rtc = null;
      for (Coordinate coord : group2D.coords)
       if (coord.getType() == Coordinate.Type.runtime)
         rtc = (CoordinateRuntime) coord;
      assert rtc != null;
      List<Double> runOffset = rtc.getRuntimesUdunits();

      // create the best time coordinates, for GroupB
      // order is preserved with Group2D
      for (Coordinate coord : group2D.coords) {
        if (coord instanceof CoordinateTimeAbstract) {
          Coordinate best = ((CoordinateTimeAbstract)coord).createBestTimeCoordinate(runOffset);
          groupB.coords.add(best);

        } else {
          groupB.coords.add(coord);
        }
      }

      // transfer variables
      for (GribCollection.VariableIndex vi2d : group2D.variList) {
        // copy vi and add to groupB
        PartitionCollection.VariableIndexPartitioned vip = result.makeVariableIndexPartitioned(groupB, vi2d, npart);
        // do not remove runtime coordinate, just set isTwoD
        vip.twot = null;

        int timeIdx = vi2d.getCoordinateIndex(Coordinate.Type.time2D);
        if (timeIdx >= 0) {
          CoordinateTime2D time2d = (CoordinateTime2D) group2D.coords.get(timeIdx);
          CoordinateTimeAbstract timeBest = (CoordinateTimeAbstract) groupB.coords.get(timeIdx);
          vip.time2runtime = time2d.makeTime2RuntimeMap(timeBest, ((PartitionCollection.VariableIndexPartitioned) vi2d).twot);
          continue;
        }

        timeIdx = vi2d.getCoordinateIndex(Coordinate.Type.time);
        if (timeIdx >= 0) {
          CoordinateTime time2d = (CoordinateTime) group2D.coords.get(timeIdx);
          CoordinateTime timeBest = (CoordinateTime) groupB.coords.get(timeIdx); // LOOK do we know these have same index ??
          vip.time2runtime = time2d.makeTime2RuntimeMap(runOffset, timeBest, ((PartitionCollection.VariableIndexPartitioned) vi2d).twot);
          continue;
        }

        timeIdx = vi2d.getCoordinateIndex(Coordinate.Type.timeIntv);
        if (timeIdx >= 0) {
          CoordinateTimeIntv time2d = (CoordinateTimeIntv) group2D.coords.get(timeIdx);
          CoordinateTimeIntv timeBest = (CoordinateTimeIntv) groupB.coords.get(timeIdx);
          vip.time2runtime = time2d.makeTime2RuntimeMap(runOffset, timeBest, ((PartitionCollection.VariableIndexPartitioned) vi2d).twot);
          continue;
        }
      }

    } // loop over groups
  }

  //////////////////////////////////////////////////////////

  public String getMagicStart() {
    return MAGIC_START;
  }

  // writing ncx2
  /*
  MAGIC_START
  version
  sizeRecords
  VariableRecords (sizeRecords bytes)
  sizeIndex
  GribCollectionIndex (sizeIndex bytes)
  */
  private boolean writeIndex(PartitionCollection pc, Formatter f) throws IOException {
    File file = pc.getIndexFile();
    if (file.exists()) {
      if (!file.delete())
        logger.error("gc2tp cant delete " + file.getPath());
    }

    try (RandomAccessFile raf = new RandomAccessFile(file.getPath(), "rw")) {
      raf.order(RandomAccessFile.BIG_ENDIAN);

      //// header message
      raf.write(getMagicStart().getBytes(CDM.utf8Charset));
      raf.writeInt(version);
      raf.writeLong(0); // no record section

      /*
      message GribCollection {
        required string name = 1;         // must be unique - index filename is name.ncx
        required string topDir = 2;       // filenames are reletive to this
        repeated MFile mfiles = 3;        // list of grib MFiles
        repeated Dataset dataset = 4;
        repeated Gds gds = 5;             // unique Gds, shared amongst datasets

        required int32 center = 6;      // these 4 fields are to get a GribTable object
        required int32 subcenter = 7;
        required int32 master = 8;
        required int32 local = 9;       // grib1 table Version

        optional int32 genProcessType = 10;
        optional int32 genProcessId = 11;
        optional int32 backProcessId = 12;

        repeated Parameter params = 20;      // not used yet

        extensions 100 to 199;
      }

      extend GribCollection {
        repeated Partition partitions = 100;
      }
       */

      GribCollectionProto.GribCollection.Builder indexBuilder = GribCollectionProto.GribCollection.newBuilder();
      indexBuilder.setName(pc.getName());
      indexBuilder.setTopDir(pc.getDirectory().getPath()); // LOOK

      // mfiles are the partition indexes
      int count = 0;
      for (PartitionCollection.Partition part : pc.partitions) {
        GribCollectionProto.MFile.Builder b = GribCollectionProto.MFile.newBuilder();
        b.setFilename(part.getIndexFilename());
        b.setLastModified(part.getLastModified());
        b.setIndex(count++);
        indexBuilder.addMfiles(b.build());
      }

      indexBuilder.setCenter(pc.getCenter());
      indexBuilder.setSubcenter(pc.getSubcenter());
      indexBuilder.setMaster(pc.getMaster());
      indexBuilder.setLocal(pc.getLocal());

      indexBuilder.setGenProcessId(pc.getGenProcessId());
      indexBuilder.setGenProcessType(pc.getGenProcessType());
      indexBuilder.setBackProcessId(pc.getBackProcessId());

      indexBuilder.setMasterRuntime(writeCoordProto(pc.getMasterRuntime()));

      //gds
      for (GribCollection.HorizCoordSys hcs : pc.horizCS)
        indexBuilder.addGds(writeGdsProto(hcs));

      // dataset
      for (GribCollection.Dataset ds : pc.datasets)
        indexBuilder.addDataset(writeDatasetProto(pc, ds));

      // extensions
      List<Integer> run2partList = new ArrayList<>();
      if (pc.run2part != null) {
        for (int part : pc.run2part) run2partList.add(part);
        indexBuilder.setExtension(PartitionCollectionProto.run2Part, run2partList);
      }

      List<PartitionCollectionProto.Partition> partProtoList = new ArrayList<>();
      for (PartitionCollection.Partition part : pc.partitions)
        partProtoList.add(writePartitionProto(part));
      indexBuilder.setExtension(PartitionCollectionProto.partitions, partProtoList);
      indexBuilder.setExtension(PartitionCollectionProto.isPartitionOfPartitions, pc.isPartitionOfPartitions);

      // write it out
      GribCollectionProto.GribCollection index = indexBuilder.build();
      byte[] b = index.toByteArray();
      NcStream.writeVInt(raf, b.length); // message size
      raf.write(b);  // message  - all in one gulp
      f.format("Grib2PartitionIndex= %d bytes file size =  %d bytes%n%n", b.length, raf.length());
    }

    return true;
  }

  /*
  message Dataset {
    required Type type = 1;
    repeated Group groups = 2;
  }
   */
  private GribCollectionProto.Dataset writeDatasetProto(PartitionCollection pc, PartitionCollection.Dataset ds) throws IOException {
    GribCollectionProto.Dataset.Builder b = GribCollectionProto.Dataset.newBuilder();

    GribCollectionProto.Dataset.Type type = GribCollectionProto.Dataset.Type.valueOf(ds.getType().toString());
    b.setType(type);

    for (GribCollection.GroupGC group : ds.groups)
      b.addGroups(writeGroupProto(pc, group));

    return b.build();
  }

  /*
   message Group {
    required uint32 gdsIndex = 1;       // index into GribCollection.gds array
    repeated Variable variables = 2;    // list of variables
    repeated Coord coords = 3;          // list of coordinates
    repeated int32 fileno = 4;          // the component files that are in this group, index into gc.mfiles

    repeated Parameter params = 20;      // not used yet
    extensions 100 to 199;
   }
    extend Group {
      repeated uint32 run2part = 100;       // runtime index to partition index
    }
   */
  private GribCollectionProto.Group writeGroupProto(PartitionCollection pc, GribCollection.GroupGC g) throws IOException {
    GribCollectionProto.Group.Builder b = GribCollectionProto.Group.newBuilder();

    b.setGdsIndex(pc.findHorizCS(g.horizCoordSys));
    b.setIsTwod(g.isTwod);

    for (GribCollection.VariableIndex vb : g.variList) {
      b.addVariables(writeVariableProto((PartitionCollection.VariableIndexPartitioned) vb));
    }

    for (Coordinate coord : g.coords) {
      switch (coord.getType()) {
        case runtime:
          b.addCoords(writeCoordProto((CoordinateRuntime) coord));
          break;
        case time:
          b.addCoords(writeCoordProto((CoordinateTime) coord));
          break;
        case timeIntv:
          b.addCoords(writeCoordProto((CoordinateTimeIntv) coord));
          break;
        case time2D:
          b.addCoords(writeCoordProto((CoordinateTime2D) coord));
          break;
        case vert:
          b.addCoords(writeCoordProto((CoordinateVert) coord));
          break;
      }
    }

    if (g.filenose != null)
      for (Integer fileno : g.filenose)
        b.addFileno(fileno);

    return b.build();
  }

  /*
  message Variable {
     required uint32 discipline = 1;
     required bytes pds = 2;          // raw pds
     required fixed32 cdmHash = 3;

     required uint64 recordsPos = 4;  // offset of SparseArray message for this Variable
     required uint32 recordsLen = 5;  // size of SparseArray message for this Variable

     repeated uint32 coordIdx = 6;    // indexes into Group.coords

     // optionally keep stats
     optional float density = 7;
     optional uint32 ndups = 8;
     optional uint32 nrecords = 9;
     optional uint32 missing = 10;

     repeated uint32 invCount = 15;      // for Coordinate TwoTimer, only 2D vars
     repeated uint32 time2runtime = 16;  // time index to runtime index, only 1D vars
     repeated Parameter params = 20;    // not used yet

     extensions 100 to 199;
   }
  extend Variable {
    repeated PartitionVariable partition = 100;
  }
   */
  private GribCollectionProto.Variable writeVariableProto(PartitionCollection.VariableIndexPartitioned vp) throws IOException {

    GribCollectionProto.Variable.Builder b = GribCollectionProto.Variable.newBuilder();

    b.setDiscipline(vp.discipline);
    b.setPds(ByteString.copyFrom(vp.rawPds));
    b.setCdmHash(vp.cdmHash);

    b.setRecordsPos(vp.recordsPos);
    b.setRecordsLen(vp.recordsLen);

    for (int idx : vp.coordIndex)
      b.addCoordIdx(idx);

    b.setDensity(vp.density);
    b.setNdups(vp.ndups);
    b.setNrecords(vp.nrecords);
    b.setMissing(vp.missing);

    if (vp.twot != null) { // only for 2D
      for (int invCount : vp.twot.getCount())
        b.addInvCount(invCount);
    }

    if (vp.time2runtime != null) { // only for 1D
      for (int idx : vp.time2runtime)
        b.addTime2Runtime(idx);
    }

        // extensions
    List<PartitionCollectionProto.PartitionVariable> pvarList = new ArrayList<>();
    for (PartitionCollection.PartitionForVariable2D pvar : vp.getPartitionsForVariable())
      pvarList.add(writePartitionVariableProto(pvar));
    b.setExtension(PartitionCollectionProto.partition, pvarList);

    return b.build();
  }

  /*
  message PartitionVariable {
    required uint32 groupno = 1;
    required uint32 varno = 2;
    required uint32 flag = 3;
    required uint32 partno = 4;

    // optionally keep stats
    optional float density = 7;
    optional uint32 ndups = 8;
    optional uint32 nrecords = 9;
    optional uint32 missing = 10;
  }
   */
  private PartitionCollectionProto.PartitionVariable writePartitionVariableProto(PartitionCollection.PartitionForVariable2D pvar) throws IOException {
    PartitionCollectionProto.PartitionVariable.Builder pb = PartitionCollectionProto.PartitionVariable.newBuilder();
    pb.setPartno(pvar.partno);
    pb.setGroupno(pvar.groupno);
    pb.setVarno(pvar.varno);
    pb.setFlag(pvar.flag);
    pb.setNdups(pvar.ndups);
    pb.setNrecords(pvar.nrecords);
    pb.setMissing(pvar.missing);
    pb.setDensity(pvar.density);

    return pb.build();
  }

  /*
message Partition {
  required string name = 1;       // name is used in TDS - eg the subdirectory when generated by TimePartitionCollections
  required string filename = 2;   // the gribCollection.ncx2 file
  required string directory = 3;   // top directory
  optional uint64 lastModified = 4;
}
  }
   */
  private PartitionCollectionProto.Partition writePartitionProto(PartitionCollection.Partition p) throws IOException {
    PartitionCollectionProto.Partition.Builder b = PartitionCollectionProto.Partition.newBuilder();

    b.setFilename(p.getIndexFilename());
    b.setName(p.getName());
    b.setDirectory(p.getDirectory());
    b.setLastModified(p.getLastModified());

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

}