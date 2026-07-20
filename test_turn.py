import asyncio
from aiortc import RTCPeerConnection, RTCConfiguration, RTCIceServer
import logging

logging.basicConfig(level=logging.INFO)

async def main():
    config = RTCConfiguration(
        iceServers=[
            RTCIceServer(
                urls=["turn:openrelay.metered.ca:80"],
                username="openrelayproject",
                credential="openrelayproject",
            )
        ]
    )
    pc = RTCPeerConnection(configuration=config)
    
    @pc.on("iceconnectionstatechange")
    def on_iceconnectionstatechange():
        print(f"ICE state: {pc.iceConnectionState}")

    @pc.on("icegatheringstatechange")
    def on_icegatheringstatechange():
        print(f"Gathering state: {pc.iceGatheringState}")
        
    print("Creating offer...")
    pc.createDataChannel("test")
    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    
    print("Waiting for gathering...")
    # Wait for gathering to complete
    for i in range(20):
        if pc.iceGatheringState == "complete":
            break
        await asyncio.sleep(0.5)
        
    print(f"Final gathering state: {pc.iceGatheringState}")
    sdp = pc.localDescription.sdp
    if "typ relay" in sdp:
        print("SUCCESS: TURN relay candidate gathered!")
    else:
        print("FAILED: No TURN relay candidate found in SDP.")
        
    await pc.close()

if __name__ == "__main__":
    asyncio.run(main())
