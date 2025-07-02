import sys
import os
from dotenv import load_dotenv
from openai import OpenAI

# Add the text_parser directory to the system path to import the fetch function
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'text_parser')))

# Import the fetch and clean function

def generate_icebreaker(cleaned_text, output_file="icebreaker.txt", temperature=0.7, org_id=None):
    """
    Fetches cleaned text from a LinkedIn company profile and generates a personalized icebreaker using OpenAI's Chat Completion API.
    
    Parameters:
        company_url (str): URL of the LinkedIn company profile (e.g., 'https://www.linkedin.com/company/microsoft').
        output_file (str): Path to save the generated icebreaker (default: 'icebreaker.txt').
        temperature (float): Sampling temperature for OpenAI API (default: 0.7).
        org_id (str, optional): OpenAI organization ID, if required.
    
    Returns:
        str: Personalized icebreaker text, or None if an error occurs.
    """
    try:
        # Load OpenAI API key and optional organization ID from .env file
        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key not found in .env file.")

        # Initialize OpenAI client
        client = OpenAI(api_key=api_key, organization=org_id)

        # Fetch and clean LinkedIn company profile text

        # if not cleaned_text:
        #     print(f"Failed to fetch or clean text")
        #     return None

        # Truncate cleaned text to fit within token limits (256 max_tokens)
        max_text_length = 3000  # Approx. 1500 chars to stay within 256 tokens with prompt
        cleaned_text = cleaned_text[:max_text_length]

        # Define system and user messages
        system_message = {
            "role": "system",
            "content": "You are a professional copywriter specializing in B2B outreach for real estate businesses."
        }
        user_message = {
            "role": "user",
            "content": f"""
            # Role
            Act as an outreach specialist for an AI technology company, specializing in crafting engaging icebreakers for email outreach to hospitality professionals in the hotel industry to promote AI agents for customer support, reservations, and operations.

            # Task
            Create compelling and personalized icebreakers for email templates to initiate contact with decision-makers in the hotel industry in North America. These icebreakers should be based on text extracted from LinkedIn company profiles or individual profiles of hotel professionals. Your goal is to make the initial email feel personalized, engaging, and relevant, encouraging a positive response to discuss AI solutions for customer support, reservations, or operations.

            ## Specifics
            - Generate icebreakers that are directly related to the content found on the LinkedIn company or individual profile provided, focusing on their work in hospitality, hotels, or related travel sectors.
            - Keep the icebreakers concise (1-2 sentences), engaging, and relevant to the hotel industry and the use of AI for customer support, reservations, or operations.
            - Personalize the icebreaker to the recipient using details such as their role, company achievements, or recent activities mentioned on LinkedIn.
            - The icebreaker should seamlessly integrate into the email template following the greeting "Hi {{name}}, {{icebreaker}}".
            - Avoid generic statements that could apply to any company or individual; focus on specifics like recent hotel openings, tech initiatives, or guest experience improvements.
            - If the LinkedIn profile text is vague or lacks detail, craft an icebreaker that shows genuine interest in learning more about their work, goals, or challenges in customer service, reservations, or operations.
            - Subtly tie the icebreaker to potential benefits of AI agents (e.g., improving guest experience, streamlining reservations, boosting operational efficiency) without being overtly salesy.
            - Target decision-makers such as General Managers, Property Managers, Directors of Operations, Revenue Managers, Guest Services Managers, Reservations Managers, IT Directors, CTOs, CIOs, VPs of Operations, CEOs, Founders, Co-Founders, Owners, or Marketing Directors in hotels or hospitality companies.

            ## Target Audience
            - **Geography**: North America
            - **Industry**: Hospitality, Hotels and Motels, Travel Agencies and Tour Operators, Online Travel Platforms and Booking Websites
            - **Company Size**: 1-10, 11-50, 51-200, 201-500, 501-1000
            - **Function**: Operations, Sales, Marketing, Information Technology, Customer Success, General Management
            - **Seniority Level**: Owner, C-Level, Director, Manager
            - **Titles**: Chief Executive Officer, Director of Operations, Marketing Director, Senior Director of Marketing, Property Manager, Co-Founder, Founder, Owner, General Manager, IT Director, Revenue Manager, Guest Services Manager, Reservations Manager, Chief Technology Officer, Chief Information Officer, VP of Operations
            - **Keywords**: AI, chatbot, customer support, reservations, guest experience, revenue management, hospitality technology

            ## Tools
            You do not have external tools to assist you. Your primary resource will be the LinkedIn company or individual profile text provided by the user, or inferred details based on the target audience’s roles, industries, and functions.

            **Usage Context**
            The icebreaker will be inserted into this email template:

            > **Hi {{name}}, {{icebreaker}}**
            **Do not include Hi {{name}} in your response, i just need the icebreaker, raw text. No "*" or quotes ("") in the end or beggining. **

            **Guidelines**
            ## Examples
            Q: LinkedIn company profile mentions a hotel chain recently opened a new property in Miami.
            A: Congrats on the new Miami property opening! How are you planning to enhance the guest experience at this exciting new location?

            Q: LinkedIn individual profile of a General Manager highlights their focus on improving guest satisfaction scores.
            A: I saw your impressive work on boosting guest satisfaction scores! What strategies are you exploring to take the guest experience to the next level?

            Q: LinkedIn company profile mentions adopting new technology to streamline operations.
            A: Your recent push to adopt new tech for operations caught my eye! What challenges are you tackling to make your hotel’s operations even smoother?

            Q: LinkedIn profile is vague but indicates the person is a Revenue Manager at a boutique hotel.
            A: As a Revenue Manager at a boutique hotel, I bet you’re always looking for ways to optimize bookings. What’s the latest project you’re excited about?

            ## Notes
            - Your responses should feel like they’re coming from a real person who has taken the time to understand the recipient’s role, achievements, or company initiatives in the hotel industry.
            - Maintain a professional yet approachable tone, avoiding overly formal language to foster a human and friendly interaction.
            - Always aim to make the recipient feel like the email was crafted specifically for them, using LinkedIn profile details to personalize the icebreaker.
            - Avoid technical AI jargon (e.g., “machine learning,” “NLP”) unless the profile explicitly mentions tech adoption; instead, use terms like “guest experience,” “reservations,” or “efficiency” to align with hospitality priorities.
            - The primary goal is to break the ice and encourage dialogue about how AI agents can address their specific needs in customer support, reservations, or operations, not to pitch the product directly in the icebreaker.
            - If the profile mentions specific challenges (e.g., staffing shortages, guest complaints), tailor the icebreaker to subtly address how AI could help without being pushy.
            - If the company profile text is empty, just generate a general icebreaker that can fit to all kind of people. 
            - If you don't understand anything, or any error happen just answer with just a space (i dont want the email to have wierd things) and never mention anything else (e.g. i couldn't provide ..., please rovide text... etc)

            Company Profile Text:
            {cleaned_text}
            """
        }

        # Call OpenAI Chat Completion API
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Cheapest model as of 2025
            messages=[system_message, user_message],
            max_tokens=300,
            temperature=temperature
        )

        # Extract the generated icebreaker
        icebreaker = response.choices[0].message.content.strip()
        print(icebreaker)
        # Save icebreaker to file
        # write_icebreaker = icebreaker + "\n\n"
        # with open(output_file, "a", encoding="utf-8") as f:
        #     f.write(write_icebreaker)
        # print(f"Icebreaker saved to {output_file}")

        return icebreaker

    except Exception as e:
        print(f"Error generating icebreaker: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Example LinkedIn company profile URL
    company_url = "https://www.linkedin.com/company/telos-furniture/"
    
    # Optional: Specify organization ID if required (replace with your org_id or None)
    org_id = os.getenv("OPENAI_ORG_ID")  # Or set to your org_id directly, e.g., "org-123456"
    
    # Generate icebreaker
    icebreaker = generate_icebreaker(company_url, temperature=0.7, org_id=org_id)
    
    if icebreaker:
        print("Generated Icebreaker:")
        print(icebreaker)
    else:
        print("Failed to generate icebreaker.")