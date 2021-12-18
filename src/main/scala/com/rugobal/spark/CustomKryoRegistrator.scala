package com.rugobal.spark

import org.apache.spark.serializer.KryoRegistrator

import com.esotericsoftware.kryo.Kryo

class CustomKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[(_, _)])
    kryo.register(classOf[Array[(_, _)]])
  }

}