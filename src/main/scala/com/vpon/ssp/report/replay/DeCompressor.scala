package com.vpon.ssp.report.replay

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.GZIPInputStream

import scala.io.Source

import org.anarres.lzo.{LzoInputStream, LzoLibrary, LzoAlgorithm}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream


trait IDecompressor {
  def decompressData(inputData: Array[Byte]): Array[Byte]
}

object GZIPDecompressor extends IDecompressor {

  def decompressData(inputData: Array[Byte]): Array[Byte] = {
    val bais = new ByteArrayInputStream(inputData)
    val zis = new GZIPInputStream(bais)
    val buf = new Array[Byte](1024)
    var num = zis.read(buf, 0, buf.length)
    val baos = new ByteArrayOutputStream()
    while (num != -1) {
      baos.write(buf, 0, num)
      num = zis.read(buf, 0, buf.length)
    }
    val outputData = baos.toByteArray()
    baos.flush()
    baos.close()
    zis.close()
    bais.close()
    outputData
  }

}

object BZIP2Decompressor extends IDecompressor {

  def decompressData(inputData: Array[Byte]): Array[Byte] = {
    val bais = new ByteArrayInputStream(inputData)
    val zis = new BZip2CompressorInputStream(bais)
    val buf = new Array[Byte](1024)
    var num = zis.read(buf, 0, buf.length)
    val baos = new ByteArrayOutputStream()
    while (num != -1) {
      baos.write(buf, 0, num)
      num = zis.read(buf, 0, buf.length)
    }
    val outputData = baos.toByteArray()
    baos.flush()
    baos.close()
    zis.close()
    bais.close()
    outputData
  }

}

object LZOPDecompressor extends IDecompressor {

  val algorithm = LzoAlgorithm.LZO1X
  val decompressor = LzoLibrary.getInstance().newDecompressor(algorithm, null)

  def decompressData(inputData: Array[Byte]): Array[Byte] = {
    val bais = new ByteArrayInputStream(inputData)
    val zis = new LzoInputStream(bais, decompressor)
    val buf = new Array[Byte](1024)
    var num = zis.read(buf, 0, buf.length)
    val baos = new ByteArrayOutputStream()
    while (num != -1) {
      baos.write(buf, 0, num)
      num = zis.read(buf, 0, buf.length)
    }
    val outputData = baos.toByteArray()
    baos.flush()
    baos.close()
    zis.close()
    bais.close()
    outputData
  }

}
