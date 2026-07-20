import asyncio
from aiortc import RTCPeerConnection, RTCConfiguration, RTCIceServer
import logging

logging.basicConfig(level=logging.INFO)

async def main():
    config = RTCConfiguration(
        iceServers=[
            RTCIceServer(
                urls=[
                    "turn:global.relay.metered.ca:80",
                    "turn:global.relay.metered.ca:80?transport=tcp",
                    "turn:global.relay.metered.ca:443",
                    "turns:global.relay.metered.ca:443?transport=tcp"
                ],
                username="b15e7638bea94865a4884140",
                credential="ThSSPf0EYid+7hHh",
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
