package dev.morling.onebrc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;

class CalculateAverageJeroenTest {

    @Test
    void parsesTemperatureTenths() {
        assertEquals(123, CalculateAverage_jeroen.parseTemperatureTenths("12.3".getBytes(StandardCharsets.UTF_8), 0, 4));
        assertEquals(-7, CalculateAverage_jeroen.parseTemperatureTenths("-0.7".getBytes(StandardCharsets.UTF_8), 0, 4));
        assertEquals(-120, CalculateAverage_jeroen.parseTemperatureTenths("-12.0".getBytes(StandardCharsets.UTF_8), 0, 5));
    }

    @Test
    void roundsMeanLikeJavaMathRound() {
        assertEquals(-1, CalculateAverage_jeroen.roundedMeanTenths(-3, 2));
        assertEquals(2, CalculateAverage_jeroen.roundedMeanTenths(3, 2));
    }

    @Test
    void computesExpectedOutputOnFixture() throws Exception {
        Path file = Files.createTempFile("1brc-fixture", ".txt");
        Files.writeString(file,
                "A;1.0\n" +
                        "A;2.0\n" +
                        "A;3.0\n" +
                        "B;-1.0\n" +
                        "B;-2.0\n" +
                        "B;-3.0\n",
                StandardCharsets.UTF_8);

        Map<String, CalculateAverage_jeroen.Stats> stats = CalculateAverage_jeroen.calculate(file, 2);
        assertEquals("{A=1.0/2.0/3.0, B=-3.0/-2.0/-1.0}", CalculateAverage_jeroen.format(stats));
    }

    @Test
    void isDeterministicAcrossWorkerCounts() throws Exception {
        Path file = Files.createTempFile("1brc-det", ".txt");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5_000; i++) {
            sb.append("Station").append(i % 37).append(';')
                    .append((i % 20) - 10).append('.').append(i % 10).append('\n');
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);

        String one = CalculateAverage_jeroen.format(CalculateAverage_jeroen.calculate(file, 1));
        String many = CalculateAverage_jeroen.format(CalculateAverage_jeroen.calculate(file, 8));

        assertEquals(one, many);
    }
}
