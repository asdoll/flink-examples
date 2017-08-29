/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.example.flink;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Collections;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.spec.DSAGenParameterSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer
 * STREAMIT_HOME/apps/benchmarks/asplos06/beamformer/streamit/BeamFormer1.str
 * for original implementations. Each StreamIt's language constructs (i.e.,
 * pipeline, filter and splitjoin) are rewritten as classes in StreamJit.
 * @author Sumanan sumanan@mit.edu
 * @since Mar 8, 2013
 */
public final class BeamFormer1 {
	private BeamFormer1() {}
	
	private static final int numChannels = 12;
	private static final int numSamples = 256;
	private static final int numBeams = 4;
	private static final int numCoarseFilterTaps = 64;
	private static final int numFineFilterTaps = 64;
	private static final int coarseDecimationRatio = 1;
	private static final int fineDecimationRatio = 2;
	private static final int numSegments = 1;
	private static final int numPostDec1 = numSamples / coarseDecimationRatio;
	private static final int numPostDec2 = numPostDec1 / fineDecimationRatio;
	private static final int mfSize = numSegments * numPostDec2;
	private static final int pulseSize = numPostDec2 / 2;
	private static final int predecPulseSize = pulseSize * coarseDecimationRatio * fineDecimationRatio;
	private static final int targetBeam = numBeams / 4;
	private static final int targetSample = numSamples / 4;
	private static final int targetSamplePostDec = targetSample / coarseDecimationRatio / fineDecimationRatio;
	private static final float dOverLambda = 0.5f;
	private static final float cfarThreshold = (float) (0.95 * dOverLambda * numChannels * (0.5 * pulseSize));
	private static final int ITEMS = 10_000_000;
	private static int tempChannel;
	private static List<Float> tmpfloat = new ArrayList<Float>();
	private static Queue<Float> tmpque = new LinkedList<Float>();

	public static void main(String[] args) throws InterruptedException {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String[] input = {String.valueOf(ITEMS)};
		DataStream<Float> text = env.fromElements(input).flatMap(new FlatMapFunction<String, Float>(){

			@Override
			public void flatMap(String list, Collector<Float> out) {
		            out.collect(Float.parseFloat(list));
			}
			
		});
		DataStream<Tuple1<Float>> floats = text.map(new ToTuple());
		for (int i = 0; i < numChannels; i++) {
			tempChannel = i;
			floats = floats.flatMap(new InputGenerate()).map(new ToTuple()).keyBy(0)
					.fold(Tuple2.of((float)2, (float)0),new BeamFirFilter()).flatMap(new Tuple2toFloats()).map(new ToTuple()).keyBy(0)
					.fold(Tuple2.of((float)4, (float)0),new BeamFirFilter()).flatMap(new Tuple2toFloats()).map(new ToTuple());
		}
		for (int i = 0; i < numBeams; i++) {
			tempChannel = i;
			floats = floats.keyBy(0).fold(Tuple2.of((float)2, (float)0),new BeamFirFilter2()).flatMap(new Tuple2toFloats()).map(new ToTuple()).keyBy(0);
		}
		
	}

	
	private static final class ToTuple implements MapFunction<Float, Tuple1<Float>> {
		@Override
		public Tuple1<Float> map(Float value) throws Exception {

			return new Tuple1<Float>(value);
		}
	}
	
	private static final class Tuple2toFloats implements FlatMapFunction<Tuple2<Float,Float>,Float> {

		@Override
		public void flatMap(Tuple2<Float, Float> cur, Collector<Float> out) throws Exception {
			out.collect(cur.f0);
			out.collect(cur.f1);			
		}
	}
	
	private static Tuple2<Float,Float> InputGenerate(int myChannel, int numberOfSamples, int tarBeam,
			int targetSample, float thresh) {
		int curSample;
		final boolean holdsTarget;
		float t1,t2;

		curSample = 0;
		holdsTarget = (tarBeam == myChannel);
		
		if (holdsTarget && (curSample == targetSample)) {
			t1 =((float) Math.sqrt(curSample * myChannel));
			t2 =((float) Math.sqrt(curSample * myChannel) + 1);
		} else {
			t1 =((float) (-Math.sqrt(curSample * myChannel)));
			t2 =((float) (-(Math.sqrt(curSample * myChannel) + 1)));
		}
		curSample++;
		if (curSample >= numberOfSamples)
			curSample = 0;
		
		return new Tuple2<Float,Float>(t1,t2) ;
	}

	private static final class InputGenerate implements FlatMapFunction<Tuple1<Float>, Float> {
		

		@Override
		public void flatMap(Tuple1<Float> arg0, Collector<Float> out) throws Exception {
			Tuple2<Float,Float> tmp = InputGenerate(tempChannel, numSamples, targetBeam, targetSample, cfarThreshold);
			out.collect(tmp.f0);
			out.collect(tmp.f1);
			
		}
	}

	private static Tuple2<Float,Float> BeamFirFilter(int numTaps, int inputLength, int decimationRatio, Tuple2<Float,Float> tmp) {
		int count = 0;
	    float[] real_weight = new float[numTaps];
		float[] imag_weight = new float[numTaps];
		float[] realBuffer = new float[numTaps];
		float[] imagBuffer = new float[numTaps];
		int numTapsMinusOne = numTaps - 1;
		int pos = 0;
		for (int j = 0; j < numTaps; j++) {
			int idx = j + 1;
			real_weight[j] = (float) (Math.sin(idx) / ((float) idx));
			imag_weight[j] = (float) (Math.cos(idx) / ((float) idx));
		}
		
		float real_curr = 0;
		float imag_curr = 0;

		realBuffer[numTapsMinusOne - pos] = tmp.f0;

		imagBuffer[numTapsMinusOne - pos] = tmp.f1;

		int modPos = numTapsMinusOne - pos;
		for (int i = 0; i < numTaps; i++) {
			real_curr += realBuffer[modPos] * real_weight[i]
					+ imagBuffer[modPos] * imag_weight[i];
			imag_curr += imagBuffer[modPos] * real_weight[i]
					+ realBuffer[modPos] * imag_weight[i];

			modPos = (modPos + 1) & numTapsMinusOne;
		}

		pos = (pos + 1) & numTapsMinusOne;

		count += decimationRatio;

		if (count == inputLength) {
			count = 0;
			pos = 0;
			for (int i = 0; i < numTaps; i++) {
				realBuffer[i] = 0;
				imagBuffer[i] = 0;
			}
		}
		return new Tuple2<Float,Float>(real_curr,imag_curr);
	}
	
	private static final class BeamFirFilter implements FoldFunction<Tuple1<Float>, Tuple2<Float,Float>> {
		@Override
		public Tuple2<Float,Float> fold(Tuple2<Float,Float> num, Tuple1<Float> current) throws Exception {
			Tuple2<Float,Float> tmp;
			if(num.f0.equals(tmpfloat.size())){
				if(num.f0.equals(2*coarseDecimationRatio)){
					tmp = BeamFirFilter(numCoarseFilterTaps, numSamples, coarseDecimationRatio,Tuple2.of(tmpfloat.get(0), tmpfloat.get(1)));
					tmpfloat.clear();
					return tmp;
				}
				if(num.f0.equals(2*fineDecimationRatio)){
					tmp = BeamFirFilter(numFineFilterTaps, numPostDec1, fineDecimationRatio,Tuple2.of(tmpfloat.get(0), tmpfloat.get(1)));
					tmpfloat.clear();
					return tmp;
				}
			}
			tmpfloat.add(current.f0);
			return num;
		}
	}
	private static final class BeamFirFilter2 implements FoldFunction<Tuple1<Float>, Tuple2<Float,Float>> {
		@Override
		public Tuple2<Float,Float> fold(Tuple2<Float,Float> num, Tuple1<Float> current) throws Exception {
			Tuple2<Float,Float> tmp;
			if(num.f0.equals(tmpfloat.size())){
					tmp = BeamFirFilter(mfSize, numPostDec2, 1,Tuple2.of(tmpfloat.get(0), tmpfloat.get(1)));
					tmpfloat.clear();
					return tmp;
			}
			tmpfloat.add(current.f0);
			return num;
		}
	}
	
	private static Tuple2<Float,Float> BeamForm(int myBeamId, int numChannels) {
		float[] real_weight;
		float[] imag_weight;		
		real_weight = new float[numChannels];
		imag_weight = new float[numChannels];
		for (int j = 0; j < numChannels; j++) {
			int idx = j + 1;
			real_weight[j] = (float) (Math.sin(idx) / ((float) (myBeamId + idx)));
			imag_weight[j] = (float) (Math.cos(idx) / ((float) (myBeamId + idx)));
		}
		float real_curr = 0;
		float imag_curr = 0;
		for (int i = 0; i < numChannels; i++) {
			float real_pop = tmpque.poll();
			float imag_pop = tmpque.poll();

			real_curr += real_weight[i] * real_pop - imag_weight[i]
					* imag_pop;
			imag_curr += real_weight[i] * imag_pop + imag_weight[i]
					* real_pop;
		}
		return new Tuple2<Float,Float>(real_curr,imag_curr);
	}

	private static final class BeamForm implements FoldFunction<Tuple1<Float>, Tuple2<Float,Float>> {
		@Override
		public Tuple2<Float,Float> fold(Tuple2<Float,Float> num, Tuple1<Float> current) throws Exception {
			Tuple2<Float,Float> tmp;
			if(num.f0.equals(tmpque.size())){
				tmp = BeamForm(tempChannel, numChannels);
				return tmp;
			}
			tmpque.add(current.f0);
			return num;
		}
	}

	private final static class Magnitude implements FoldFunction<Tuple1<Float>, Tuple1<Float>> {
		@Override
		public Tuple1<Float> fold(Tuple1<Float> f1, Tuple1<Float> cur) throws Exception {
			if(!f1.f0.equals(0))
				return Tuple1.of((float)Math.sqrt(f1.f0 * f1.f0 + cur.f0 * cur.f0));
			return cur;
		}
	}

}