package com.hotels.styx.api.stream;

import com.hotels.styx.api.Buffer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;


public class ByteStreamTest {
    private Buffer buf1;
    private Buffer buf2;
    private Buffer buf3;
    private TestSubscriber<Buffer> testSubscriber;
    private TestSubscriber<String> stringSubscriber;

    @BeforeMethod
    public void setUp() {
        buf1 = new Buffer("a", UTF_8);
        buf2 = new Buffer("b", UTF_8);
        buf3 = new Buffer("c", UTF_8);
        testSubscriber = new TestSubscriber<>(0);
        stringSubscriber = new TestSubscriber<>(0);
    }

    @Test
    public void publishesContent() {
        ByteStream stream = new ByteStream(new RxContentPublisher(Observable.just(buf1, buf2, buf3)));
        RxContentConsumer consumer = new RxContentConsumer(stream.publisher());

        testSubscriber.requestMore(255);
        consumer.consume().subscribe(testSubscriber);

        assertThat(testSubscriber.getOnNextEvents().size(), is(3));
        assertThat(testSubscriber.getOnErrorEvents().size(), is(0));
        assertThat(testSubscriber.getOnCompletedEvents().size(), is(1));
    }

    @Test
    public void publisherBackpressure() {
        ByteStream stream = new ByteStream(new RxContentPublisher(Observable.just(buf1, buf2, buf3)));
        RxContentConsumer consumer = new RxContentConsumer(stream.publisher());

        consumer.consume().subscribe(testSubscriber);

        assertThat(testSubscriber.getOnNextEvents().size(), is(0));

        testSubscriber.requestMore(1);
        assertThat(testSubscriber.getOnNextEvents().size(), is(1));

        testSubscriber.requestMore(1);
        assertThat(testSubscriber.getOnNextEvents().size(), is(2));

        testSubscriber.requestMore(1);
        assertThat(testSubscriber.getOnNextEvents().size(), is(3));
        assertThat(testSubscriber.getOnErrorEvents().size(), is(0));
        assertThat(testSubscriber.getOnCompletedEvents().size(), is(1));
    }

    @Test
    public void mapsContent() {
        ByteStream stream = new ByteStream(new RxContentPublisher(Observable.just(buf1, buf2, buf3)));

        RxContentConsumer consumer = new RxContentConsumer(stream.map(this::toUpperCase).publisher());

        consumer.consume()
                .map(this::decodeUtf8String)
                .subscribe(stringSubscriber);

        stringSubscriber.requestMore(100);

        assertThat(stringSubscriber.getOnNextEvents(), contains("A", "B", "C"));
        assertThat(stringSubscriber.getOnErrorEvents().size(), is(0));
        assertThat(stringSubscriber.getOnCompletedEvents().size(), is(1));
    }

    @Test
    public void discardsContent() {
        ByteStream stream = new ByteStream(new RxContentPublisher(Observable.just(buf1, buf2, buf3)));

        RxContentConsumer consumer = new RxContentConsumer(stream.discard().publisher());

        consumer.consume().subscribe(testSubscriber);
        testSubscriber.requestMore(100);

        assertThat(testSubscriber.getOnNextEvents(), empty());
        assertThat(testSubscriber.getOnErrorEvents(), empty());
        assertThat(testSubscriber.getOnCompletedEvents().size(), is(1));
    }

    @Test
    public void aggregatesContent() throws ExecutionException, InterruptedException {
        ByteStream stream = new ByteStream(new RxContentPublisher(Observable.just(buf1, buf2, buf3)));

        Buffer aggregated = stream.aggregate(100).get();
        assertThat(decodeUtf8String(aggregated), is("abc"));
    }


    private String decodeUtf8String(Buffer buffer) {
        return new String(buffer.content(), UTF_8);
    }

    private Buffer toUpperCase(Buffer buffer) {
        return new Buffer(decodeUtf8String(buffer).toUpperCase(), UTF_8);
    }

    @Test(enabled = false)
    public void flatMapscontent() {
    }

    @Test(enabled = false)
    public void peeksContent() {
    }

}
