package org.apache.spark.sql.hbase

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{FilterBase, FilterList}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.expressions.{Row, _}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.hbase.DataTypeUtils._
import org.apache.spark.sql.{SchemaRDD, StructType}

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

private[hbase] case class HBaseRelation(
                                         @transient configuration: Configuration, //HBaseConfig?
                                         @transient hbaseContext: HBaseSQLContext,
                                         @transient connection: HConnection,
                                         tableName: String, hbaseNamespace: String,
                                         hbaseTableName: String, allColumns: Seq[Column],
                                         keyColumns: Seq[Column], nonKeyColumns: Seq[NonKeyColumn]
                                         )
  extends LeafNode {
  self: Product =>

  @transient lazy val handle: HTable = new HTable(configuration, hbaseTableName)
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  //  @transient lazy val connection = HConnectionManager.createConnection(configuration)

  lazy val partitionKeys = keyColumns.map {
    case col: Column =>
      AttributeReference(col.sqlName, col.dataType, nullable = true)()
  } //catalogTable.rowKey.asAttributes

  lazy val attributes = nonKeyColumns.map {
    case col: Column =>
      AttributeReference(col.sqlName, col.dataType, nullable = true)()
  } //catalogTable.columns.asAttributes

  //  lazy val colFamilies = nonKeyColumns.map(_.family).distinct
  //  lazy val applyFilters = false

  def closeHTable() = handle.close

  override def output: Seq[Attribute] = {
    allColumns.map {
      case colName =>
        (partitionKeys union attributes).find(_.name == colName).get
    }
  }

  //TODO-XY:ADD getPrunedPartitions
  lazy val partitions: Seq[HBasePartition] = {
    import scala.collection.JavaConverters._
    val tableNameInSpecialClass = TableName.valueOf(hbaseNamespace, tableName)
    val regionLocations = connection.locateRegions(tableNameInSpecialClass)
    val partSeq = regionLocations.asScala
      .zipWithIndex.map { case (hregionLocation, index) =>
      val regionInfo = hregionLocation.getRegionInfo
      new HBasePartition(index, HBasePartitionBounds(
        Some(regionInfo.getStartKey),
        Some(regionInfo.getEndKey)),
        Some(Seq(hregionLocation.getServerName.getHostname)(0)))
    }
    partSeq
  }

  def getPrunedPartitions(partionPred: Option[Expression]): Option[Seq[HBasePartition]] = {
    //TODO-XY:Use the input parameter
    Option(partitions)
  }

  //  def buildFilter(rowKeyPredicates: Seq[Expression],
  //                  colPredicates: Seq[Expression]) = {
  //    var colFilters: Option[FilterList] = None
  //    if (HBaseStrategies.PushDownPredicates) {
  //      // TODO: rewrite the predicates based on Catalyst Expressions
  //      // TODO: Do column pruning based on only the required colFamilies
  //      val filters: HBaseSQLFilters = new HBaseSQLFilters(colFamilies,
  //        rowKeyPredicates, colPredicates)
  //      colFilters = filters.createColumnFilters
  //      // TODO: Perform Partition pruning based on the rowKeyPredicates
  //    }
  //    colFilters
  //  }
  //
  //  def buildPut(schema: StructType, row: Row): Put = {
  //    val rkey = RowKeyParser.createKeyFromCatalystRow(schema, keyColumns, row)
  //    val p = new Put(rkey)
  //    DataTypeUtils.catalystRowToHBaseRawVals(schema, row, nonKeyColumns).zip(nonKeyColumns)
  //      .map { case (raw, col) => p.add(s2b(col.family), s2b(col.qualifier), raw)
  //    }
  //    p
  //  }
  //
  //  def buildScanner(split: Partition): Scan = {
  //    val hbPartition = split.asInstanceOf[HBasePartition]
  //    val scan = if (applyFilters) {
  //      new Scan(hbPartition.bounds.start.get,
  //        hbPartition.bounds.end.get)
  //    } else {
  //      new Scan
  //    }
  //    if (applyFilters) {
  //      colFamilies.foreach { cf =>
  //        scan.addFamily(s2b(cf))
  //      }
  //    }
  //    scan
  //  }

  def getRowPrefixPredicates(predicates: Seq[Expression]) = {
    //Filter out all predicates that only deal with partition keys, these are given to the
    //hive table scan operator to be used for partition pruning.
    val partitionKeyIds = AttributeSet(partitionKeys)
    val (rowKeyPredicates, _ /*otherPredicates*/ ) = predicates.partition {
      _.references.subsetOf(partitionKeyIds)
    }

    // Find and sort all of the rowKey dimension elements and stop as soon as one of the
    // composite elements is not found in any predicate
    val loopx = new AtomicLong
    val foundx = new AtomicLong
    val rowPrefixPredicates = for {pki <- partitionKeyIds
                                   if ((loopx.incrementAndGet >= 0)
                                     && rowKeyPredicates.flatMap {
                                     _.references
                                   }.contains(pki)
                                     && (foundx.incrementAndGet == loopx.get))
                                   attrib <- rowKeyPredicates.filter {
                                     _.references.contains(pki)
                                   }
    } yield attrib
    rowPrefixPredicates
  }


  def isOnlyBinaryComparisonPredicates(predicates: Seq[Expression]) = {
    predicates.forall(_.isInstanceOf[BinaryPredicate])
  }

  class HBaseSQLFilters(colFamilies: Seq[String],
                        rowKeyPreds: Seq[Expression],
                        opreds: Seq[Expression])
    extends FilterBase {
    @transient val logger = Logger.getLogger(getClass.getName)

    def createColumnFilters(): Option[FilterList] = {
      val colFilters: FilterList =
        new FilterList(FilterList.Operator.MUST_PASS_ALL)
      //      colFilters.addFilter(new HBaseRowFilter(colFamilies,
      //          catalogTable.rowKeyColumns.columns,
      //        rowKeyPreds.orNull))
      opreds.foreach {
        case preds: Seq[Expression] =>
          // TODO; re-do the predicates logic using expressions
          //          new SingleColumnValueFilter(s2b(col.colName.family.get),
          //          colFilters.addFilter(f)
          //        }
          colFilters
      }
      Some(colFilters)
    }
  }

  /**
   * Presently only a sequence of AND predicates supported. TODO(sboesch): support simple tree
   * of AND/OR predicates
   */
  class HBaseRowFilter(colFamilies: Seq[String],
                       rkCols: Seq[Column],
                       rowKeyPreds: Seq[Expression]
                        ) extends FilterBase {
    @transient val logger = Logger.getLogger(getClass.getName)

    override def filterRowKey(rowKey: Array[Byte], offset: Int, length: Int): Boolean = {

      if (!isOnlyBinaryComparisonPredicates(rowKeyPreds)) {
        false // Presently only simple binary comparisons supported
      } else {
        //        def catalystToHBaseColumnName(catColName: String) = {
        //          nonKeyColumns.find(_.sqlName == catColName)
        //        }
        //
        //        def getName(expression: NamedExpression) = expression.asInstanceOf[NamedExpression].name
        //
        //        val rowPrefixPreds = getRowPrefixPredicates(rowKeyPreds
        //          .asInstanceOf[Seq[BinaryExpression]])
        // TODO: fix sorting of rowprefix preds
        val rowKeyColsMap = RowKeyParser.parseRowKeyWithMetaData(rkCols, rowKey)
        val result = rowKeyPreds.forall { p =>
          p.eval(Row(rowKeyColsMap.values.map {
            _._2
          })).asInstanceOf[Boolean]
        }
        result
      }
    }

    //    override def isFamilyEssential(name: Array[Byte]): Boolean = {
    //      colFamilies.contains(new String(name, HBaseByteEncoding).toLowerCase())
    //    }
  }

  def rowKeysFromRows(schemaRdd: SchemaRDD) = {
    schemaRdd.map { r: Row =>
      RowKeyParser.createKeyFromCatalystRow(
        schemaRdd.schema,
        keyColumns,
        r)
    }
  }


  /**
   * Trait for RowKeyParser's that convert a raw array of bytes into their constituent
   * logical column values
   *
   */
  trait AbstractRowKeyParser {
    def createKey(rawBytes: Seq[HBaseRawType], version: Byte): HBaseRawType

    def parseRowKey(rowKey: HBaseRawType): Seq[HBaseRawType]

    def parseRowKeyWithMetaData(rkCols: Seq[Column], rowKey: HBaseRawType)
    : SortedMap[ColumnName, (Column, Any)]
  }

  case class RowKeySpec(offsets: Seq[Int], version: Byte = RowKeyParser.Version1)

  // TODO(Bo): replace the implementation with the null-byte terminated string logic
  object RowKeyParser extends AbstractRowKeyParser with Serializable {
    val Version1 = 1.toByte
    val VersionFieldLen = 1
    // Length in bytes of the RowKey version field
    val DimensionCountLen = 1
    // One byte for the number of key dimensions
    val MaxDimensions = 255
    val OffsetFieldLen = 2

    // Two bytes for the value of each dimension offset.
    // Therefore max size of rowkey is 65535.  Note: if longer rowkeys desired in future
    // then simply define a new RowKey version to support it. Otherwise would be wasteful
    // to define as 4 bytes now.
    def computeLength(keys: Seq[HBaseRawType]) = {
      VersionFieldLen + keys.map(_.length).sum +
        OffsetFieldLen * keys.size + DimensionCountLen
    }

    override def createKey(keys: Seq[HBaseRawType], version: Byte = Version1): HBaseRawType = {
      val barr = new Array[Byte](computeLength(keys))
      val arrayx = new AtomicInteger(0)
      barr(arrayx.getAndAdd(VersionFieldLen)) = version // VersionByte

      // Remember the starting offset of first data value
      val valuesStartIndex = new AtomicInteger(arrayx.get)

      // copy each of the dimension values in turn
      keys.foreach { k => copyToArr(barr, k, arrayx.getAndAdd(k.length))}

      // Copy the offsets of each dim value
      // The valuesStartIndex is the location of the first data value and thus the first
      // value included in the Offsets sequence
      keys.foreach { k =>
        copyToArr(barr,
          short2b(valuesStartIndex.getAndAdd(k.length).toShort),
          arrayx.getAndAdd(OffsetFieldLen))
      }
      barr(arrayx.get) = keys.length.toByte // DimensionCountByte
      barr
    }

    def copyToArr[T](a: Array[T], b: Array[T], aoffset: Int) = {
      b.copyToArray(a, aoffset)
    }

    def short2b(sh: Short): Array[Byte] = {
      val barr = Array.ofDim[Byte](2)
      barr(0) = ((sh >> 8) & 0xff).toByte
      barr(1) = (sh & 0xff).toByte
      barr
    }

    def b2Short(barr: Array[Byte]) = {
      val out = (barr(0).toShort << 8) | barr(1).toShort
      out
    }

    def createKeyFromCatalystRow(schema: StructType, keyCols: Seq[Column], row: Row) = {
      val rawKeyCols = DataTypeUtils.catalystRowToHBaseRawVals(schema, row, keyCols)
      createKey(rawKeyCols)
    }

    def getMinimumRowKeyLength = VersionFieldLen + DimensionCountLen

    override def parseRowKey(rowKey: HBaseRawType): Seq[HBaseRawType] = {
      assert(rowKey.length >= getMinimumRowKeyLength,
        s"RowKey is invalid format - less than minlen . Actual length=${rowKey.length}")
      assert(rowKey(0) == Version1, s"Only Version1 supported. Actual=${rowKey(0)}")
      val ndims: Int = rowKey(rowKey.length - 1).toInt
      val offsetsStart = rowKey.length - DimensionCountLen - ndims * OffsetFieldLen
      val rowKeySpec = RowKeySpec(
        for (dx <- 0 to ndims - 1)
        yield b2Short(rowKey.slice(offsetsStart + dx * OffsetFieldLen,
          offsetsStart + (dx + 1) * OffsetFieldLen))
      )

      val endOffsets = rowKeySpec.offsets.tail :+ (rowKey.length - DimensionCountLen - 1)
      val colsList = rowKeySpec.offsets.zipWithIndex.map { case (off, ix) =>
        rowKey.slice(off, endOffsets(ix))
      }
      colsList
    }

    //TODO
    override def parseRowKeyWithMetaData(rkCols: Seq[Column], rowKey: HBaseRawType):
    SortedMap[ColumnName, (Column, Any)] = {
      import scala.collection.mutable.HashMap

      //      val rowKeyVals = parseRowKey(rowKey)
      //    val rmap = rowKeyVals.zipWithIndex.foldLeft(new HashMap[ColumnName, (Column, Any)]()) {
      //      case (m, (cval, ix)) =>
      //        m.update(rkCols(ix).toColumnName, (rkCols(ix),
      //          hbaseFieldToRowField(cval, rkCols(ix).dataType)))
      //        m
      //    }
      //    TreeMap(rmap.toArray: _*)(Ordering.by { cn: ColumnName => rmap(cn)._1.ordinal})
      //      .asInstanceOf[SortedMap[ColumnName, (Column, Any)]]
      null
    }

    def show(bytes: Array[Byte]) = {
      val len = bytes.length
      //      val out = s"Version=${bytes(0).toInt} NumDims=${bytes(len - 1)} "
    }

  }

}
