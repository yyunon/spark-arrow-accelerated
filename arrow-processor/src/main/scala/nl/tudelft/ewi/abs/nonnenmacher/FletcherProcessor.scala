package nl.tudelft.ewi.abs.nonnenmacher

import java.util.logging.Logger

import org.apache.arrow.memory.ArrowBuf
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.NativeLibraryLoader
import org.apache.arrow.gandiva.expression.ArrowTypeHelper
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}

import scala.collection.JavaConverters._

class FletcherProcessor(schema_container: Array[Schema]) extends ClosableFunction[VectorSchemaRoot, Long] {

  private val log = Logger.getLogger(classOf[FletcherProcessor].getName)

  private var isClosed = false
  private val procId: Long = {
    NativeLibraryLoader.load()
    val schemaAsBytes = Array(ArrowTypeHelper.arrowSchemaToProtobuf(schema_container(0)).toByteArray, 
                              ArrowTypeHelper.arrowSchemaToProtobuf(schema_container(1)).toByteArray)
  // TODO: Change this ASAP
    initFletcherProcessor(schemaAsBytes(0), schemaAsBytes(1))
  }

  def apply(rootIn_1: VectorSchemaRoot,rootIn_2: VectorSchemaRoot ): Long = {
    // TODO:
    // Is there a more intelligent way to do all these? am I using these apis correctly haha?
    // A way to automate multiple parquet files or not use multiple at all
    val buffersIn_1 = BufferDescriptor(rootIn_1)
    val buffersIn_2 = BufferDescriptor(rootIn_2)

    buffersIn_1.assertAre64ByteAligned()
    buffersIn_2.assertAre64ByteAligned()

    val r = join(procId, buffersIn_1.rowCount, buffersIn_1.addresses, buffersIn_1.sizes, buffersIn_2.rowCount, buffersIn_2.addresses, buffersIn_2.sizes)
    buffersIn.close()
    r
  }


  // TODO: Change this ASAP
  @native private def initFletcherProcessor(schemaAsBytes_1: Array[Byte], schemaAsBytes_2: Array[Byte]): Long

  //@native private def reduce(procId: Long, rowNumbers: Int, inBufAddrs: Array[Long], inBufSized: Array[Long]): Long;

  @native private def join(procId: Long, rowNumbers_1: Int, inBufAddrs_1: Array[long], inBufSized_1: Array[Long], rowNumbers_2: Int, inBufAddrs_2: Array[long], inBufSized_2: Array[Long] ): Long;

  @native private def close(procId: Long): Unit;

  private case class BufferDescriptor(root: VectorSchemaRoot) {
    def close(): Any = recordBatch.close();

    lazy val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch
    lazy val buffers: scala.collection.mutable.Buffer[ArrowBuf] = recordBatch.getBuffers.asScala
    lazy val rowCount: Int = root.getRowCount
    lazy val addresses: Array[Long] = buffers.map(_.memoryAddress()).toArray
    lazy val sizes: Array[Long] = buffers.map(_.readableBytes()).toArray


    def assertAre64ByteAligned(): Unit = {

      val sb = new StringBuilder()
      var isAligned = true

      buffers.foreach { b =>
        sb.append(s"Addr: ${b.memoryAddress().toHexString} % 64 = ${b.memoryAddress() % 64} ")
        sb.append(s"Capacity: ${b.capacity()} % 64 = ${b.capacity() % 64} ")
        sb.append("\n")

        isAligned &= b.memoryAddress() % 64 == 0
        isAligned &= b.capacity() % 64 == 0
      }

      if (!isAligned) {
        log.warning("Buffers are not aligned. \n" + sb.toString())
      }
    }
  }

  override def close(): Unit = {
    if (!isClosed) {
      isClosed = true
      close(procId)
    }
  }
}
