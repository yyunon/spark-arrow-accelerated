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

class FletcherProcessor(schema: Array[Schema], hash: Array[VectorSchemaRoot]) extends ClosableFunction[VectorSchemaRoot, Long] {

  private val log = Logger.getLogger(classOf[FletcherProcessor].getName)

  private var isClosed = false
  val procId: Long = {
    NativeLibraryLoader.load()
    val schemaAsBytes = Array(ArrowTypeHelper.arrowSchemaToProtobuf(schema(0)).toByteArray, 
                              ArrowTypeHelper.arrowSchemaToProtobuf(schema(1)).toByteArray)
    // TODO: Change this later
    initFletcherProcessor(schemaAsBytes(0), schemaAsBytes(1))
  }

  def apply(rootIn: VectorSchemaRoot): Long = {

    val buffersIn = BufferDescriptor(rootIn)
    val hashedKeys = BufferDescriptor(hash(0)) // Hash will be less than 500! Be careful later on!

    buffersIn.assertAre64ByteAligned()
    hashedKeys.assertAre64ByteAligned()

    // TODO:
    // Seperate these later
    val b = broadcast(procId, hashedKeys.rowCount, hashedKeys.addresses, hashedKeys.sizes)
    val r = join(procId, buffersIn.rowCount, buffersIn.addresses, buffersIn.sizes)
    buffersIn.close()
    hashedKeys.close()
    r
  }
  @native private def initFletcherProcessor(schemaAsBytes_1: Array[Byte], schemaAsBytes_2: Array[Byte]): Long

  @native private def broadcast(procId: Long, rowNumbers: Int, inBufAddrs: Array[Long], inBufSized: Array[Long]): Long;

  @native private def join(procId: Long, rowNumbers: Int, inBufAddrs: Array[Long], inBufSized: Array[Long]): Long;

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

