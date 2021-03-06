/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.directives.npl;

import co.cask.directives.nlp.Stemming;
import co.cask.directives.nlp.internal.PorterStemmer;
import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link PorterStemmer} and {@link Stemming}
 */
public class StemmingTest {

  @Test
  public void testPorterStemming() throws Exception {
    PorterStemmer stemmer = new PorterStemmer();
    String[] i = new String[]{
      "How",
      "are",
      "you",
      "doing",
      "do",
      "you",
      "have",
      "apples"
    };
    List<String> o = stemmer.process(Arrays.asList(i));
    Assert.assertTrue(o.size() > 1);
  }

  @Test
  public void testStemming() throws Exception {
    String[] directives = new String[] {
      "stemming words",
    };

    List<Row> rows = Arrays.asList(
      new Row("words", Arrays.asList("how", "are", "you", "doing", "do", "you", "have", "apples"))
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(Arrays.asList("how", "ar", "you", "do", "do", "you", "have", "appl"),
                        rows.get(0).getValue("words_porter"));
  }

  @Test
  public void testStringStemming() throws Exception {
    String[] directives = new String[] {
      "stemming words",
    };

    List<Row> rows = Arrays.asList(
      new Row("words", "how are you doing ? do you have apples")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(Arrays.asList("how", "ar", "you", "do", "do", "you", "have", "appl"),
                        rows.get(0).getValue("words_porter"));
  }

}
