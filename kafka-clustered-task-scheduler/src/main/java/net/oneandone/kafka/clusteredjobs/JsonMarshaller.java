package net.oneandone.kafka.clusteredjobs;

import java.io.IOException;
import java.time.Instant;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import net.oneandone.kafka.clusteredjobs.api.NodeTaskInformation;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

/**
 * Used to marshal and unmarshall kafka events
 */
public class JsonMarshaller {

    public static GsonBuilder gsonBuilder = new GsonBuilder();
    static  {
        gsonBuilder.registerTypeAdapter(NodeTaskInformation.TaskInformation.class,
                new TypeAdapter<NodeTaskInformation.TaskInformation>() {

                    @Override
                    public void write(final JsonWriter jsonWriter, final NodeTaskInformation.TaskInformation taskInformation) throws
                            IOException {
                        jsonWriter.beginObject();
                        jsonWriter.name("taskName");
                        jsonWriter.value(taskInformation.getTaskName());
                        jsonWriter.name("state");
                        jsonWriter.value(taskInformation.getState().name());
                        if (taskInformation.getNodeName().isPresent()) {
                            jsonWriter.name("nodeName");
                            jsonWriter.value(taskInformation.getNodeName().get());
                        }
                        jsonWriter.endObject();
                    }

                    @Override
                    public NodeTaskInformation.TaskInformation read(final JsonReader jsonReader) throws IOException {
                        jsonReader.beginObject();
                        jsonReader.nextName();
                        String taskName = jsonReader.nextString();
                        jsonReader.nextName();
                        StateEnum state = StateEnum.valueOf(jsonReader.nextString());
                        String nodeName = null;
                        if (jsonReader.hasNext()) {
                            jsonReader.nextName();
                            nodeName = jsonReader.nextString();
                        }
                        jsonReader.endObject();
                        NodeTaskInformation.TaskInformation ti = new NodeTaskInformationImpl.TaskInformationImpl(
                                taskName, state, nodeName);
                        return ti;
                    }
                });
        gsonBuilder.registerTypeAdapter(Instant.class, new TypeAdapter<Instant>(){
            @Override
            public void write(final JsonWriter jsonWriter, final Instant instant) throws IOException {
                jsonWriter.value(instant.toEpochMilli());
            }

            @Override
            public Instant read(final JsonReader jsonReader) throws IOException {
                long millisEpoch = jsonReader.nextLong();
                return Instant.ofEpochMilli(millisEpoch);
            }
        });

    }
    public static Gson gson = gsonBuilder.create();

    JsonMarshaller() {
        
    }
}
