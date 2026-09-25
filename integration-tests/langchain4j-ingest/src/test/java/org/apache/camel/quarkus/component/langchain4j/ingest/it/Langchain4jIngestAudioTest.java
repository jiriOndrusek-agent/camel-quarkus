/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.quarkus.component.langchain4j.ingest.it;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.sound.sampled.AudioFileFormat;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code audio} modality: a WAV dropped into the watched directory is embedded whole, as one
 * vector, by the audio-capable test model, the file name as the document id. The fixture is
 * generated, so no binary is checked in.
 */
@QuarkusTest
class Langchain4jIngestAudioTest {

    @Test
    void audioFileIsEmbeddedWholeAndFoundByTheSameClip() throws Exception {
        byte[] tone = wav(200);

        RestAssured.given().contentType(ContentType.BINARY)
                .body(tone)
                .post("/langchain4j-ingest/binary/audio/tone.wav")
                .then().statusCode(204);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    List<Map<String, String>> hits = searchByClip(tone);
                    assertEquals(1, hits.size(), "one vector per audio file");
                    assertEquals("audio", hits.get(0).get("pipeline"));
                    assertEquals("tone.wav", hits.get(0).get("documentId"));
                    // the placeholder segment carries the id as its text
                    assertEquals("tone.wav", hits.get(0).get("text"));
                });

        // a different clip embeds to a different vector, so it is no exact hit
        assertTrue(searchByClip(wav(120)).isEmpty(), "another clip must not match the stored one");
    }

    /** Query by audio: the clip is embedded with the same model and the store searched for exact hits. */
    private static List<Map<String, String>> searchByClip(byte[] clip) {
        return RestAssured.given().contentType(ContentType.BINARY)
                .body(clip)
                .post("/langchain4j-ingest/search/audio")
                .then()
                .statusCode(200)
                .extract().jsonPath().getList("");
    }

    /** A mono 16 kHz PCM WAV of the given length holding a 440 Hz tone. */
    static byte[] wav(int millis) throws IOException {
        AudioFormat format = new AudioFormat(16_000f, 16, 1, true, false);
        int frames = (int) (format.getSampleRate() * millis / 1000);
        byte[] pcm = new byte[frames * 2];
        for (int i = 0; i < frames; i++) {
            short sample = (short) (Math.sin(2 * Math.PI * 440 * i / format.getSampleRate()) * Short.MAX_VALUE / 4);
            pcm[2 * i] = (byte) sample;
            pcm[2 * i + 1] = (byte) (sample >> 8);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AudioSystem.write(new AudioInputStream(new ByteArrayInputStream(pcm), format, frames),
                AudioFileFormat.Type.WAVE, out);
        return out.toByteArray();
    }
}
