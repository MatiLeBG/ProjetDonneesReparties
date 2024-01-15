package io;

import interfaces.NetworkReaderWriter;
import interfaces.KV;

public class NetworkReaderWriterImpl implements NetworkReaderWriter{

    private String fileName;

    public NetworkReaderWriterImpl(String fname){
            this.fileName = fname;
    }
    
    public KV read(){

    }

    public void write(KV remoteKV){

    }

    public void openServer(){

    }

    public void openClient(){

    }

    public void closeClient(){

    }

    public void closeServer(){

    }

    public NetworkReaderWriter accept(){
        
    }

}
