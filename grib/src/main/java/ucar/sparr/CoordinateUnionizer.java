package ucar.sparr;

import thredds.featurecollection.FeatureCollectionConfig;
import ucar.nc2.grib.TimeCoord;
import ucar.nc2.grib.collection.*;
import ucar.nc2.grib.grib2.Grib2Record;
import ucar.nc2.time.CalendarDate;
import ucar.nc2.time.CalendarPeriod;

import java.util.*;

/**
 * Create the overall coordinate across the same variable in different partitions
 *
 * @author John
 * @since 12/10/13
 */
public class CoordinateUnionizer {
  FeatureCollectionConfig.GribIntvFilter intvFilter;
  int varId;

  public CoordinateUnionizer(int varId, FeatureCollectionConfig.GribIntvFilter intvFilter) {
    this.intvFilter = intvFilter;
    this.varId = varId;
  }

  List<Coordinate> unionCoords = new ArrayList<>();
  CoordinateND<GribCollection.Record> result;

  CoordinateBuilder runtimeBuilder ;
  CoordinateBuilder timeBuilder;
  CoordinateBuilder timeIntvBuilder;
  CoordinateBuilder vertBuilder;
  Time2DUnionBuilder time2DBuilder;

  public void addCoords(List<Coordinate> coords) {
    Coordinate runtime = null;
    for (Coordinate coord : coords) {
      switch (coord.getType()) {
        case runtime:
          if (runtimeBuilder == null) runtimeBuilder = new CoordinateRuntime.Builder();
          runtimeBuilder.addAll(coord);
          runtime = coord;
          break;
        case time:
          CoordinateTime time = (CoordinateTime) coord;
          if (timeBuilder == null) timeBuilder = new CoordinateTime.Builder(coord.getCode(), time.getTimeUnit(), time.getRefDate());
          timeBuilder.addAll(coord);
          break;
        case timeIntv:
          CoordinateTimeIntv timeIntv = (CoordinateTimeIntv) coord;
          if (timeIntvBuilder == null) timeIntvBuilder = new CoordinateTimeIntv.Builder(null, coord.getCode(), timeIntv.getTimeUnit(), timeIntv.getRefDate());
          timeIntvBuilder.addAll(intervalFilter((CoordinateTimeIntv)coord));
          break;
        case vert:
          if (vertBuilder == null) vertBuilder = new CoordinateVert.Builder(coord.getCode());
          vertBuilder.addAll(coord);
          break;
        case time2D:
          CoordinateTime2D time2D = (CoordinateTime2D) coord;
          if (time2DBuilder == null) time2DBuilder = new Time2DUnionBuilder(time2D.isTimeInterval(), time2D.getTimeUnit(), coord.getCode());
          time2DBuilder.addAll(time2D);

          // debug
          CoordinateRuntime runtimeFrom2D = time2D.getRuntimeCoordinate();
          if (!runtimeFrom2D.equals(runtime))
            System.out.println("HEY");
          break;
      }
    }
  }

  private List<TimeCoord.Tinv> intervalFilter(CoordinateTimeIntv coord) {
    if (intvFilter == null) return coord.getTimeIntervals();
    List<TimeCoord.Tinv> result = new ArrayList<>();
    for (TimeCoord.Tinv tinv : coord.getTimeIntervals()) {
      if (intvFilter.filterOk(varId, tinv.getIntervalSize(), 0))
        result.add(tinv);
    }
    return result;
  }

      /* true means remove
  private boolean filterOut(Grib2Record gr, FeatureCollectionConfig.GribIntvFilter intvFilter) {
    int[] intv = tables.getForecastTimeIntervalOffset(gr);
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
      int discipline = gr.getIs().getDiscipline();
      Grib2Pds pds = gr.getPDS();
      int category = pds.getParameterCategory();
      int number = pds.getParameterNumber();
      int id = (discipline << 16) + (category << 8) + number;

      int prob = Integer.MIN_VALUE;
      if (pds.isProbability()) {
        Grib2Pds.PdsProbability pdsProb = (Grib2Pds.PdsProbability) pds;
        prob = (int) (1000 * pdsProb.getProbabilityUpperLimit());
      }
      return intvFilter.filterOut(id, haveLength, prob);
    }
    return false;
  }  */


  public List<Coordinate> finish() {
    if (runtimeBuilder != null)
      unionCoords.add(runtimeBuilder.finish());
    else
      System.out.println("HEY missing runtime");

    if (timeBuilder != null)
      unionCoords.add(timeBuilder.finish());
    else if (timeIntvBuilder != null)
      unionCoords.add(timeIntvBuilder.finish());
    else if (time2DBuilder != null)
      unionCoords.add(time2DBuilder.finish());
    else
      System.out.println("HEY missing time");

    if (vertBuilder != null)
      unionCoords.add(vertBuilder.finish());

    result = new CoordinateND<>(unionCoords);
    return unionCoords;
  }

  /*
   * Reindex with shared coordinates and return new CoordinateND
   * @param prev  previous
   * @return new CoordinateND containing shared coordinates and sparseArray for the new coordinates
   *
  public void addIndex(CoordinateND<GribCollection.Record> prev) {
    result.reindex(prev);
  }

  public CoordinateND<GribCollection.Record> getCoordinateND() {
    return result;
  } */

  private class Time2DUnionBuilder extends CoordinateBuilderImpl<Grib2Record> {
    boolean isTimeInterval;
    CalendarPeriod timeUnit;
    int code;
    SortedMap<CalendarDate, CoordinateTimeAbstract> timeMap = new TreeMap<>();

    public Time2DUnionBuilder(boolean isTimeInterval, CalendarPeriod timeUnit, int code) {
      this.isTimeInterval = isTimeInterval;
      this.timeUnit = timeUnit;
      this.code = code;
    }

    @Override
    public void addAll(Coordinate coord) {
      if (coord.getValues() != null)
        super.addAll(coord);
      CoordinateTime2D coordT2D = (CoordinateTime2D) coord;
      for (Coordinate tcoord : coordT2D.getTimes()) {             // possible duplicate runtimes from different partitions
        CoordinateTimeAbstract times = (CoordinateTimeAbstract) tcoord;
        timeMap.put(times.getRefDate(), times);                   // later partitions will override
      }
    }

    @Override
    public Object extract(Grib2Record gr) {
      throw new RuntimeException();
    }

    @Override
    public Coordinate makeCoordinate(List<Object> values) {

      List<CalendarDate> runtimes = new ArrayList<>();
      List<Coordinate> times = new ArrayList<>();
      for( CalendarDate cd : timeMap.keySet()) {
        runtimes.add(cd);
        times.add(timeMap.get(cd));
      }

      List<CoordinateTime2D.Time2D> vals = new ArrayList<>(values.size());
      for (Object val : values) vals.add( (CoordinateTime2D.Time2D) val);
      Collections.sort(vals);

      return new CoordinateTime2D(code, timeUnit, vals, new CoordinateRuntime(runtimes), times);
    }

  }

}