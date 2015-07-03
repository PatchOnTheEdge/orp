///*
// * The MIT License (MIT)
// *
// * Copyright (c) 2015 Ilya Verbitskiy, Patrick Probst
// *
// * Permission is hereby granted, free of charge, to any person obtaining a copy
// * of this software and associated documentation files (the "Software"), to deal
// * in the Software without restriction, including without limitation the rights
// * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// * copies of the Software, and to permit persons to whom the Software is
// * furnished to do so, subject to the following conditions:
// *
// * The above copyright notice and this permission notice shall be included in all
// * copies or substantial portions of the Software.
// *
// * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// * SOFTWARE.
// */
//
//package de.tuberlin.orp.common.akka.serializer;
//
//import akka.serialization.JSerializer;
//import com.esotericsoftware.kryo.Kryo;
//import com.esotericsoftware.kryo.Serializer;
//import com.esotericsoftware.kryo.io.Input;
//import com.esotericsoftware.kryo.io.Output;
//import com.esotericsoftware.kryo.pool.KryoFactory;
//import com.esotericsoftware.kryo.pool.KryoPool;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import io.verbit.ski.core.json.Json;
//
//public class KryoSerializer extends JSerializer {
//  private KryoPool kryoPool;
//
//  public KryoSerializer() {
//
//    KryoFactory kryoFactory = () -> {
//      Serializer serializer = new Serializer() {
//        @Override
//        public void write(Kryo kryo, Output output, Object o) {
//          byte[] bytes = Json.toBinary(o);
//          output.writeInt(bytes.length);
//          output.write(bytes);
//        }
//
//        @Override
//        public Object read(Kryo kryo, Input input, Class aClass) {
//          int length = input.readInt();
//          byte[] bytes = input.readBytes(length);
//          return Json.parse(bytes);
//        }
//      };
//
//      Kryo kryo = new Kryo();
//      kryo.register(JsonNode.class, serializer);
//      kryo.register(ObjectNode.class, serializer);
//      return kryo;
//    };
//
//    KryoPool.Builder builder = new KryoPool.Builder(kryoFactory);
//    kryoPool = builder.softReferences().build();
//  }
//
//  @Override
//  public Object fromBinaryJava(byte[] bytes, Class<?> manifest) {
//    return kryoPool.run(kryo -> kryo.readObject(new Input(bytes), manifest));
//  }
//
//  @Override
//  public int identifier() {
//    return 2001;
//  }
//
//  @Override
//  public byte[] toBinary(Object o) {
//    return kryoPool.run(kryo -> {
//			Output output = new Output(2000, 10000);
//			kryo.writeObject(output, o);
//			return output.toBytes();
//		});
//  }
//
//  @Override
//  public boolean includeManifest() {
//    return true;
//  }
//}
