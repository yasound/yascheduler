Yascheduler application
=======================

This application is designed to control the audio streamers of the Yasound platform.  


. Structure
-----------

Yasound's main thread has a list of events and is awaken regularly to handle the events.  

The events can be:  
- prepare a new track for a radio  
- a track is played in a radio  

The communication with the streamers uses redis messages.  
- Yascheduler owns a thread which subscribes to the radis channel 'yascheduler'  
- Every streamer registers to yascheduler and sends its identifier. Yascheduler uses this identifier to send messages in redis channel 'yastream.identifier', so every streamer listens to its own redis channel.  

Yascheduler sends 'ping' messages regularly to streamers in order to check if they are still alive. The streamers have to answer with a 'pong' message.  

   
. Communication
---------------

Messages are formatted using json:  
`    {  
        'type': "message_type_defined_below",  
        'param_name_1': param_value_1,  
        'param_name_2': param_value_2,  
        'param_name_3': param_value_3,  
    }`  

##_- messages from the streamer:_


***
__register streamer__  
sent when a streamer is created  
yascheduler stores it and includes it in the 'ping' loop  

_type_ : 'register_streamer'  

_params_:  
`streamer` : streamer identifier


***
__unregister streamer__  
yascheduler removes it from mongodb, clean all events, radios and listeners info, and notify yaapp that every radio handled by this streamer has stopped playing   

_type_ : 'unregister_streamer'  

_params_:  
`streamer` : streamer identifier


***
__pong__  
It the response to ping message,  
The streamer notifies yascheduler that it is alive

_type_ : 'pong'  

_params_:  
`streamer` : streamer identifier


***
__user authentication__  
The streamer asks for a user authentication.  
2 methods are provided:  
- passing an `auth_token` param  
- passing `username` and `api_key` params (this method is implemented for backward compatibility, new clients must send an `auth_token` to the streamer instead of `username` + `api_key`)  

In response, a `user_authentication` message is sent from yascheduler to the streamer (see 'messages from yascheduler' section)


_type_ : 'user_authentication'  

_params_:  
`streamer` : streamer identifier  

`auth_token` : string  

or  

`username` : username  
`api_key` : api_key  


***
__play radio__  
The streamer wants to play a radio,  
if another streamer already handles the radio, 'radio_exists' message is sent in response  
if the radio didn't exist, it is created and 'radio_started' message is sent to the streamer

_type_ : 'play_radio'  

_params_:  
`streamer` : streamer identifier  
`radio_uuid` : radio uuid


***
__stop radio__  
The streamer wants to stop a radio,  
if the radio existed, 'radio_stopped' message is sent to the streamer in response

_type_ : 'stop_radio'  

_params_:  
`streamer` : streamer identifier  
`radio_uuid` : radio uuid


***
__register listener__  
A client starts listening to the radio

_type_ : 'register_listener'  

_params_:   
`radio_uuid` : radio uuid  
`session_id` : listening session unique id provided by the streamer  
`user_id` : user id, can be None if it's an anonymous client   


***
__unregister listener__  
A client stops listening to a radio

_type_ : 'unregister_listener'  

_params_:   
`session_id` : same listening session id as the one passed to `register_client`  








