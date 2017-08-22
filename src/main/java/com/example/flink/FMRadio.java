package com.example.flink;

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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.spec.DSAGenParameterSpec;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
@SuppressWarnings("serial")
public final class FMRadio {
	private static final float samplingRate = 250000000; // 250 MHz sampling
	// rate is
	// sensible
	private static final float cutoffFrequency = 108000000; // guess...
	// doesn't FM
	// freq max at
	// 108 Mhz?
	private static final float maxAmplitude = 27000;
	private static final float bandwidth = 10000;
	// determine where equalizer cuts. Note that <eqBands> is the
	// number of CUTS; there are <eqBands>-1 bands, with parameters
	// held in slots 1..<eqBands> of associated arrays.
	private static final float low = 55;
	private static final float high = 1760;
	private static final int band = 7;
	private static int tap = 128;
	private static int calculate = -1;
	private static int calculate2 = 1;

	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String[] input = binaryToFloat("data/fmradio.in");
		DataStream<String> text = env.fromElements(input);
		DataStream<Tuple1<Float>> floats = text.flatMap(new FlatMapFunction<String, Float>(){

			@Override
			public void flatMap(String list, Collector<Float> out) {
		            out.collect(Float.parseFloat(list));
			}
			
		}).map(new MapFunction<Float, Tuple1<Float>>() {
			@Override
			public Tuple1<Float> map(Float value) throws Exception {

				return new Tuple1<Float>(value);
			}
		});
		int[][] bandsTaps = {
				{7, 128},
				{11, 64},
				{5, 64},
				{7, 64},
				{9, 64},
				{5, 128},
				{9, 128},
			};
		for(int[] z:bandsTaps){
			int bands = z[0];
			int taps = z[1];
			float[] eqCutoff = new float[bands];
			float[] eqGain = new float[bands];
			for (int i = 0; i < bands; i++)
				// have exponentially spaced cutoffs
				eqCutoff[i] = (float) Math.exp(i
						* (Math.log(high) - Math.log(low)) / (bands - 1)
						+ Math.log(low));

			// first gain doesn't really correspond to a band
			eqGain[0] = 0;
			for (int i = 1; i < bands; i++) {
				// the gain grows linearly towards the center bands
				float val = (((float) (i - 1)) - (((float) (bands - 2)) / 2.0f)) / 5.0f;
				eqGain[i] = val > 0 ? 2.0f - val : 2.0f + val;
			}
			tap = taps;
			DataStream<Float> out = floats.keyBy(0).countWindowAll((long)5+tap).fold((float)0,new LowPassFilterFold())
					.union(floats.keyBy(0).countWindowAll(3).fold((float)0, new FMDemodulator()));
			

			out.print();

			break;
		}
		
		env.execute("FMRadio");
		
	}
	

	public static String[] binaryToFloat(String path) throws IOException{
		 File file = new File("data/fmradio.in");  // change to whatever you want for input.

	      int ch;
	      StringBuffer strContent = new StringBuffer("");
	      // Instead of a string buffer you might want to create an
	      //  output file to hold strContent.
	      // strContent is probably going to be... messy :-)


	      FileInputStream fin = fin = new FileInputStream(file);

	     // int charCnt = 0;
	    //  int readableCnt = 0;
	    //  int cutoff = -1; // for testing, set to -1 for all input.
	      while( (ch = fin.read()) != -1) {
	       //  ++charCnt;
	       //  if( cutoff != -1 && charCnt >= cutoff ) {
	      //      System.out.println("debug: Hit cutoff="+cutoff);
	      //      break;
	      //   }
	      //   char readable = '.'; // default to smth for not-so-readable; replace w/your favorite char here.
	         // lots of different ways to test this.
	         // If your data is relatively simple, you might want to define
	         // "readable" as anything from ascii space through newline.
	            strContent.append((float)ch).append(" ");
	        //    readable = (char)ch;
	       //     ++readableCnt;
	      //   System.out.printf("%6d: ch=%04d 0x%04x %c\n", charCnt, ch, ch, readable);
	      }
	      fin.close();
		
		return strContent.toString().split(" ");
		
	}


	private static float[] LowPassFilter(float rate, float cutoff, int taps, int decimation) {

		float[] coeff = new float[taps];
		int i;
		float m = taps - 1;
		float w = (float) (2 * Math.PI * cutoff / rate);
		for (i = 0; i < taps; i++)
			if (i - m / 2 == 0)
				coeff[i] = (float) (w / Math.PI);
			else
				coeff[i] = (float) (Math.sin(w * (i - m / 2)) / Math.PI
						/ (i - m / 2) * (0.54 - 0.46 * Math.cos(2 * Math.PI
						* i / m)));
		return coeff;
	}

	private static final class LowPassFilterFold implements FoldFunction<Tuple1<Float>, Float> {
		private int taps=tap, decimation=4;
		private float[] coeff;
		
		@Override
		public Float fold(Float value, Tuple1<Float> current) throws Exception {
			coeff = LowPassFilter(samplingRate, cutoffFrequency, tap, 4);
			calculate++;
			return value + current.f0 * coeff[calculate%(tap)];
		}
		
	}
	
	private static final class FMDemodulator implements FoldFunction<Tuple1<Float>, Float> {

		@Override
		public Float fold(Float value, Tuple1<Float> current) throws Exception {
			if(calculate2 == -1){
				value = current.f0;
				calculate2 = 0;
			}else
				if(calculate2 == 0){
				value = value * current.f0;
				calculate2 = 1;
			}else
				if(calculate2 == 1){
				value = (float) (FMDemodulator(samplingRate,maxAmplitude, bandwidth) * Math.atan(value));
				calculate2 = -1;
			}
				
			return value;
		}
	}
	
	private static float FMDemodulator(float sampRate, float max, float bandwidth) {
		return (float) (max * (sampRate / (bandwidth * Math.PI)));
	}
	
	private static final class Subtractor implements FoldFunction<Tuple1<Float>, Float> {

		@Override
		public Float fold(Float value, Tuple1<Float> current) throws Exception {
			if(value!=0) return current.f0 - value;
			return current.f0;
		}
	}

	// Inlined into BandPassFilter constructor, since it isn't used elsewhere.
	// private static class BPFCore extends Splitjoin<Float, Float> {
	// public <T extends Object, U extends Object> BPFCore(float rate, float
	// low, float high, int taps) {
	// super(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>(),
	// new LowPassFilter(rate, low, taps, 0),
	// new LowPassFilter(rate, high, taps, 0));
	// }
	// }


	private static final class Amplifier implements FoldFunction<Tuple1<Float>, Float> {

		@Override
		public Float fold(Float value, Tuple1<Float> current) throws Exception {
			return current.f0*value;
		}
	}

	private static final class Equalizer implements FoldFunction<Tuple1<Float>, Float> {
		private final float rate;
		private final int bands;
		private final float[] cutoffs, gains;
		private final int taps;

		private Equalizer(float rate, final int bands, float[] cutoffs, float[] gains,
				int taps) {
			this.rate = rate;
			this.bands = bands;
			this.cutoffs = cutoffs;
			this.gains = gains;
			this.taps = taps;

			if (cutoffs.length != bands || gains.length != bands)
				throw new IllegalArgumentException();
		}

		@Override
		public Float fold(Float value, Tuple1<Float> current) throws Exception {
			return value+current.f0;
		}
	}
}

